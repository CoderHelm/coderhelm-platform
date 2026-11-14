//! Closing runs by hand, one or up to [`MAX_BULK`] at once, and reopening
//! them.
//!
//! A person closes a run when they are done with it on purpose (not needed,
//! a duplicate, done by hand, the wrong ticket). Closing:
//!
//! - sets the run `closed` with who, when and why (`common::run_status`),
//!   which stops any work the way a cancel does: every worker check and
//!   conditional write treats `closed` like `cancelled`;
//! - frees the ticket lock, so a later reopen or edit of the ticket can start
//!   a fresh run (the trigger gate decides when);
//! - stops the auto-merge gate on the run's PRs; the CI-fix loop stops on the
//!   status itself;
//! - leaves the PR on GitHub alone unless the person ticks "Also close PR",
//!   which closes it with a comment;
//! - notes it on a Jira ticket: "Closed by X: reason".
//!
//! Reopening shows the run as it was (or as waiting on a person, when it was
//! working) and starts nothing; Retry starts work again.
//!
//! The decisions are pure ([`plan_close`], [`plan_reopen`], [`bulk_ids`]) and
//! tested below; the handlers read, write and call out.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{extract::State, http::StatusCode, Extension, Json};
use common::merge_gate::{self, Actor};
use common::run_status::{self, CloseCheck};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;

/// Most runs one bulk close takes.
pub const MAX_BULK: usize = 100;
/// Runs closed at the same time in a bulk close.
const PARALLEL: usize = 8;
/// Tries at the conditional write when the run changed status in between.
const WRITE_TRIES: usize = 3;

fn s(item: &Item, key: &str) -> String {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn n(item: &Item, key: &str) -> u64 {
    item.get(key)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// The run's key: always the caller's team, so a run of another team reads
/// as not found.
pub fn run_key(team_id: &str, run_id: &str) -> Item {
    HashMap::from([
        ("team_id".to_string(), attr_s(team_id)),
        ("run_id".to_string(), attr_s(run_id)),
    ])
}

/// One pull request a run opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrRef {
    pub owner: String,
    pub repo: String,
    pub number: u64,
}

/// What closing one run does, decided from the record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClosePlan {
    NotFound,
    AlreadyClosed,
    Refused(String),
    Close(CloseSteps),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseSteps {
    /// The status read; the write happens only while it still holds.
    pub from: String,
    /// The ticket whose lock to free.
    pub ticket_lock: Option<String>,
    /// Every PR the run opened: auto-merge stops on each.
    pub prs: Vec<PrRef>,
    /// The PRs to close on GitHub (only when asked, and only open ones).
    pub close_prs: Vec<PrRef>,
    /// The Jira ticket to note the close on.
    pub jira_ticket: Option<String>,
    pub installation_id: u64,
}

/// The PRs a run opened: its PR links (multi-repo runs have several), or its
/// one PR number on its repo.
pub fn run_prs(item: &Item) -> Vec<PrRef> {
    fn push(out: &mut Vec<PrRef>, p: PrRef) {
        if p.number > 0 && !out.contains(&p) {
            out.push(p);
        }
    }
    let mut out: Vec<PrRef> = Vec::new();
    let urls = item
        .get("pr_urls")
        .and_then(|v| v.as_l().ok())
        .map(|l| {
            l.iter()
                .filter_map(|v| v.as_s().ok().cloned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for url in urls.iter().chain(std::iter::once(&s(item, "pr_url"))) {
        if let Some((owner, repo, number)) = common::pr_outcome::parse_pr_url(url) {
            push(
                &mut out,
                PrRef {
                    owner,
                    repo,
                    number,
                },
            );
        }
    }
    if out.is_empty() {
        if let Some((owner, repo)) = s(item, "repo").split_once('/') {
            push(
                &mut out,
                PrRef {
                    owner: owner.to_string(),
                    repo: repo.to_string(),
                    number: n(item, "pr_number"),
                },
            );
        }
    }
    out
}

/// What closing the run in `item` does. `close_pr`: the person also asked to
/// close its PRs on GitHub.
pub fn plan_close(item: Option<&Item>, close_pr: bool) -> ClosePlan {
    let Some(item) = item else {
        return ClosePlan::NotFound;
    };
    let status = s(item, "status");
    match run_status::close_check(&status) {
        CloseCheck::AlreadyClosed => return ClosePlan::AlreadyClosed,
        CloseCheck::Refused(why) => return ClosePlan::Refused(why),
        CloseCheck::Close => {}
    }
    let prs = run_prs(item);
    // A PR that merged or closed already is left alone.
    let pr_done = matches!(s(item, "pr_outcome").as_str(), "merged" | "closed");
    let close_prs = if close_pr && !pr_done {
        prs.clone()
    } else {
        Vec::new()
    };
    let ticket = s(item, "ticket_id");
    ClosePlan::Close(CloseSteps {
        from: status,
        ticket_lock: (!ticket.is_empty()).then(|| ticket.clone()),
        prs,
        close_prs,
        jira_ticket: (s(item, "ticket_source") == "jira" && !ticket.is_empty()).then_some(ticket),
        installation_id: n(item, "installation_id"),
    })
}

/// The write that closes the run, while its status is still `from`.
pub struct CloseWrite {
    pub expression: &'static str,
    pub condition: &'static str,
    pub values: Item,
}

pub fn close_write(
    run_id: &str,
    from: &str,
    closer: &Closer,
    reason: Option<&str>,
    now: &str,
) -> CloseWrite {
    let mut values = HashMap::from([
        (":closed".to_string(), attr_s(run_status::CLOSED)),
        (
            ":sri".to_string(),
            attr_s(&format!("{}#{run_id}", run_status::CLOSED)),
        ),
        (":t".to_string(), attr_s(now)),
        (":by".to_string(), attr_s(&closer.email)),
        (":name".to_string(), attr_s(&closer.name)),
        (":from".to_string(), attr_s(from)),
    ]);
    let expression = match reason {
        Some(r) => {
            values.insert(":why".to_string(), attr_s(r));
            "SET #s = :closed, status_run_id = :sri, updated_at = :t, closed_at = :t, \
             closed_by = :by, closed_by_name = :name, closed_from = :from, close_reason = :why"
        }
        None => {
            "SET #s = :closed, status_run_id = :sri, updated_at = :t, closed_at = :t, \
             closed_by = :by, closed_by_name = :name, closed_from = :from REMOVE close_reason"
        }
    };
    CloseWrite {
        expression,
        condition: "attribute_exists(run_id) AND #s = :from",
        values,
    }
}

/// What reopening one run does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReopenPlan {
    NotFound,
    /// It isn't closed (its status, for the message).
    NotClosed(String),
    Reopen {
        to: &'static str,
    },
}

pub fn plan_reopen(item: Option<&Item>) -> ReopenPlan {
    let Some(item) = item else {
        return ReopenPlan::NotFound;
    };
    let status = s(item, "status");
    if status != run_status::CLOSED {
        return ReopenPlan::NotClosed(status);
    }
    ReopenPlan::Reopen {
        to: run_status::reopen_status(&s(item, "closed_from")),
    }
}

/// What a run reopened as `needs_input` waits for: what it waited for before
/// it was closed, else a Retry.
pub fn reopened_wait(item: Option<&Item>) -> common::run_wait::WaitingOn {
    use common::run_wait::WaitingOn;
    let asked = item
        .and_then(|i| i.get(common::run_questions::FIELD_QUESTIONS))
        .and_then(|v| v.as_s().ok())
        .and_then(|j| serde_json::from_str::<common::run_questions::Asked>(j).ok());
    item.and_then(WaitingOn::of_item)
        .or_else(|| asked.and_then(|a| WaitingOn::answer(&a.questions, a.comment_url.as_deref())))
        .unwrap_or_else(|| {
            WaitingOn::fixed(
                "Press Retry to run it again, or close it",
                "It was reopened after being closed, and nothing is running on it.",
                "",
            )
        })
}

/// The status-index sort key of a reopened run. A failed run keeps the
/// reason and pass it failed with; its key is the one `run_fail` writes.
fn reopened_status_run_id(to: &str, run_id: &str) -> String {
    if to == common::run_fail::STATUS {
        common::run_fail::status_run_id(run_id)
    } else {
        format!("{to}#{run_id}")
    }
}

/// The ids of a bulk close: trimmed, no blanks or repeats, at most
/// [`MAX_BULK`].
pub fn bulk_ids(ids: &[String]) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    for id in ids {
        let id = id.trim();
        if id.is_empty() || out.iter().any(|o| o == id) {
            continue;
        }
        if id.len() > 64 {
            return Err("A run id is too long.".into());
        }
        out.push(id.to_string());
    }
    if out.is_empty() {
        return Err("Choose at least one run to close.".into());
    }
    if out.len() > MAX_BULK {
        return Err(format!("Close at most {MAX_BULK} runs at once."));
    }
    Ok(out)
}

/// The person closing, as each place names them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Closer {
    /// Kept on the record for the audit.
    pub email: String,
    /// Shown on the dashboard: their name, GitHub login or email.
    pub name: String,
    /// Shown on GitHub and Jira: their name or `@login`, never an email.
    pub public: String,
    pub login: Option<String>,
}

pub fn closer(claims: &Claims, profile_name: Option<&str>) -> Closer {
    let name = profile_name.map(str::trim).filter(|n| !n.is_empty());
    let login = claims
        .github_login
        .as_deref()
        .map(str::trim)
        .filter(|l| !l.is_empty());
    Closer {
        email: claims.email.clone(),
        name: name.or(login).unwrap_or(claims.email.as_str()).to_string(),
        public: match (name, login) {
            (Some(n), _) => n.to_string(),
            (None, Some(l)) => format!("@{l}"),
            (None, None) => String::new(),
        },
        login: login.map(str::to_string),
    }
}

/// How one close went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Closed,
    AlreadyClosed,
    Refused,
    NotFound,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseResult {
    pub run_id: String,
    pub outcome: Outcome,
    /// For a person to read.
    pub message: String,
    /// PRs closed on GitHub.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub prs_closed: Vec<u64>,
    /// PRs it was asked to close but could not.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub prs_not_closed: Vec<u64>,
}

impl CloseResult {
    fn new(run_id: &str, outcome: Outcome, message: impl Into<String>) -> Self {
        CloseResult {
            run_id: run_id.to_string(),
            outcome,
            message: message.into(),
            prs_closed: Vec::new(),
            prs_not_closed: Vec::new(),
        }
    }

    /// Closed now or before: the run is closed.
    pub fn ok(&self) -> bool {
        matches!(self.outcome, Outcome::Closed | Outcome::AlreadyClosed)
    }
}

/// The bulk answer: every result, in the order asked, and the counts.
pub fn bulk_answer(results: &[CloseResult]) -> Value {
    let count = |o: Outcome| results.iter().filter(|r| r.outcome == o).count();
    json!({
        "results": results,
        "closed": count(Outcome::Closed),
        "already_closed": count(Outcome::AlreadyClosed),
        "failed": results.iter().filter(|r| !r.ok()).count(),
    })
}

#[derive(Debug, Default, Deserialize)]
pub struct CloseBody {
    #[serde(default)]
    pub reason: Option<String>,
    /// Also close the run's PR on GitHub.
    #[serde(default)]
    pub close_pr: bool,
}

#[derive(Debug, Deserialize)]
pub struct BulkCloseBody {
    pub run_ids: Vec<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub close_pr: bool,
}

/// What every close in one request shares.
struct Ctx<'a> {
    state: &'a AppState,
    team_id: &'a str,
    closer: Closer,
    reason: Option<String>,
    close_pr: bool,
    /// The team's Jira comment hook, read once.
    jira_comment_url: Option<String>,
    tokens: tokio::sync::Mutex<HashMap<u64, Option<String>>>,
}

async fn load_run(state: &AppState, team_id: &str, run_id: &str) -> Result<Option<Item>, String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .set_key(Some(run_key(team_id, run_id)))
        .consistent_read(true)
        .send()
        .await
        .map(|o| o.item)
        .map_err(|e| common::run_fail::error_text(&e))
}

pub(crate) async fn profile_name(state: &AppState, claims: &Claims) -> Option<String> {
    let id = claims.sub.strip_prefix("USER#").unwrap_or(&claims.sub);
    state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("USER#{id}")))
        .projection_expression("#n")
        .expression_attribute_names("#n", "name")
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .map(|i| s(&i, "name"))
        .filter(|n| !n.trim().is_empty())
}

async fn jira_comment_url(state: &AppState, team_id: &str) -> Option<String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("JIRA#config"))
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .map(|i| s(&i, "add_comment_url"))
        .filter(|u| !u.is_empty())
}

impl Ctx<'_> {
    async fn token(&self, installation_id: u64) -> Option<String> {
        let mut cache = self.tokens.lock().await;
        if let Some(t) = cache.get(&installation_id) {
            return t.clone();
        }
        let t = match crate::auth::github_app::get_installation_token(self.state, installation_id)
            .await
        {
            Ok(t) => Some(t),
            Err(e) => {
                warn!(installation_id, error = %e, "close: no GitHub installation token");
                None
            }
        };
        cache.insert(installation_id, t.clone());
        t
    }

    /// Close one run: the status write, then the rest (best effort; each
    /// step logs what it couldn't do).
    async fn close_one(&self, run_id: &str) -> CloseResult {
        for _ in 0..WRITE_TRIES {
            let item = match load_run(self.state, self.team_id, run_id).await {
                Ok(i) => i,
                Err(e) => {
                    warn!(run_id, error = %e, "close: could not read the run");
                    return CloseResult::new(
                        run_id,
                        Outcome::Failed,
                        "The run could not be read. Try again.",
                    );
                }
            };
            let steps = match plan_close(item.as_ref(), self.close_pr) {
                ClosePlan::NotFound => {
                    return CloseResult::new(run_id, Outcome::NotFound, "No such run on this team.")
                }
                ClosePlan::AlreadyClosed => {
                    return CloseResult::new(
                        run_id,
                        Outcome::AlreadyClosed,
                        "It was closed already.",
                    )
                }
                ClosePlan::Refused(why) => {
                    return CloseResult::new(run_id, Outcome::Refused, format!("Not closed: {why}"))
                }
                ClosePlan::Close(steps) => steps,
            };
            let now = chrono::Utc::now().to_rfc3339();
            let w = close_write(
                run_id,
                &steps.from,
                &self.closer,
                self.reason.as_deref(),
                &now,
            );
            let res = self
                .state
                .dynamo
                .update_item()
                .table_name(&self.state.config.runs_table_name)
                .set_key(Some(run_key(self.team_id, run_id)))
                .update_expression(w.expression)
                .condition_expression(w.condition)
                .expression_attribute_names("#s", "status")
                .set_expression_attribute_values(Some(w.values))
                .send()
                .await;
            match res {
                Ok(_) => return self.after_close(run_id, &steps).await,
                Err(e)
                    if e.as_service_error()
                        .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
                {
                    // Its status moved on in between: read it again.
                    continue;
                }
                Err(e) => {
                    warn!(run_id, error = %common::run_fail::error_text(&e), "close: write failed");
                    return CloseResult::new(
                        run_id,
                        Outcome::Failed,
                        "The run could not be closed. Try again.",
                    );
                }
            }
        }
        CloseResult::new(
            run_id,
            Outcome::Failed,
            "The run kept changing while closing it. Try again.",
        )
    }

    async fn after_close(&self, run_id: &str, steps: &CloseSteps) -> CloseResult {
        info!(run_id, from = %steps.from, close_pr = self.close_pr, "Run closed by a person");
        common::decision_log::on_run_acted(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            self.team_id,
            run_id,
            common::decision_log::Outcome::CancelledRun,
        )
        .await;
        let mut out = CloseResult::new(run_id, Outcome::Closed, "Closed.");
        if let Some(ticket) = &steps.ticket_lock {
            super::api::delete_ticket_lock(self.state, self.team_id, ticket).await;
        }
        // A security fix run: the findings it claimed are released now.
        if let Some(ticket) = &steps.ticket_lock {
            super::security::on_run_ended(self.state, self.team_id, ticket).await;
        }
        let actor = Actor {
            by: self
                .closer
                .login
                .clone()
                .unwrap_or_else(|| self.closer.email.clone()),
            is_login: self.closer.login.is_some(),
            via: merge_gate::VIA_DASHBOARD,
        };
        let now = chrono::Utc::now().timestamp();
        for pr in &steps.prs {
            if let Err(e) = merge_gate::stop(
                &self.state.dynamo,
                &self.state.config.settings_table_name,
                self.team_id,
                &pr.owner,
                &pr.repo,
                pr.number,
                &actor,
                now,
            )
            .await
            {
                warn!(run_id, pr = pr.number, error = %e, "close: could not stop auto-merge");
            }
        }
        if !steps.close_prs.is_empty() {
            let installation_id = if steps.installation_id > 0 {
                steps.installation_id
            } else {
                super::api::get_team_installation_id(self.state, self.team_id)
                    .await
                    .unwrap_or(0)
            };
            let token = if installation_id > 0 {
                self.token(installation_id).await
            } else {
                None
            };
            let comment = run_status::pr_close_comment(&self.closer.public, self.reason.as_deref());
            for pr in &steps.close_prs {
                let closed = match &token {
                    Some(t) => close_pr_on_github(self.state, t, pr, &comment).await,
                    None => false,
                };
                if closed {
                    out.prs_closed.push(pr.number);
                } else {
                    out.prs_not_closed.push(pr.number);
                }
            }
            if !out.prs_not_closed.is_empty() {
                out.message =
                    "Closed. GitHub didn't close the pull request; close it there.".into();
            }
        }
        if let (Some(ticket), Some(url)) = (&steps.jira_ticket, &self.jira_comment_url) {
            let note = run_status::ticket_note(&self.closer.public, self.reason.as_deref());
            let say = common::conversation::Say::status("run", note);
            if let Err(e) = super::conversation::jira_at(
                self.state,
                self.team_id,
                url,
                ticket,
                say,
                "status",
                "Closed",
            )
            .await
            {
                warn!(run_id, error = %e, "close: Jira note failed");
            }
        }
        out
    }
}

/// Comment on the PR, then close it. True when GitHub closed it.
async fn close_pr_on_github(state: &AppState, token: &str, pr: &PrRef, comment: &str) -> bool {
    let api = format!("https://api.github.com/repos/{}/{}", pr.owner, pr.repo);
    let gh = |req: reqwest::RequestBuilder| {
        req.header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
    };
    let say = common::conversation::Say::status("closed", comment);
    if let Err(e) =
        super::conversation::github(state, token, &pr.owner, &pr.repo, pr.number, say).await
    {
        warn!(pr = pr.number, error = %e, "close: PR comment failed");
    }
    match gh(state.http.patch(format!("{api}/pulls/{}", pr.number)))
        .json(&json!({ "state": "closed" }))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => true,
        Ok(r) => {
            warn!(
                pr = pr.number,
                status = r.status().as_u16(),
                "close: GitHub refused to close the PR"
            );
            false
        }
        Err(e) => {
            warn!(pr = pr.number, error = %e, "close: PR close failed");
            false
        }
    }
}

async fn ctx<'a>(
    state: &'a AppState,
    claims: &'a Claims,
    reason: Option<&str>,
    close_pr: bool,
) -> Ctx<'a> {
    let (name, jira) = tokio::join!(
        profile_name(state, claims),
        jira_comment_url(state, &claims.team_id)
    );
    Ctx {
        state,
        team_id: &claims.team_id,
        closer: closer(claims, name.as_deref()),
        reason: reason.and_then(run_status::close_reason),
        close_pr,
        jira_comment_url: jira,
        tokens: tokio::sync::Mutex::new(HashMap::new()),
    }
}

fn status_of(outcome: Outcome) -> StatusCode {
    match outcome {
        Outcome::Closed | Outcome::AlreadyClosed => StatusCode::OK,
        Outcome::Refused => StatusCode::CONFLICT,
        Outcome::NotFound => StatusCode::NOT_FOUND,
        Outcome::Failed => StatusCode::BAD_GATEWAY,
    }
}

/// POST /api/runs/:run_id/close — close one run. Closing a closed run again
/// changes nothing and answers 200; a run that is already over answers 409
/// with the reason.
pub async fn close_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
    body: Option<Json<CloseBody>>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    claims.require_role(1)?; // member+
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let ctx = ctx(&state, &claims, body.reason.as_deref(), body.close_pr).await;
    let result = ctx.close_one(&run_id).await;
    Ok((status_of(result.outcome), Json(json!(result))))
}

/// What closing a covered run's superseded pull request does, decided from
/// the run's record: the pull request to close, or why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupersededPlan {
    NotFound,
    /// Nothing to close, with why (said as is).
    Nothing(String),
    AlreadyClosed,
    Close {
        record: Box<common::stop_rules::covering::Record>,
        pr: PrRef,
        installation_id: u64,
    },
}

pub fn plan_close_superseded(item: Option<&Item>) -> SupersededPlan {
    use common::stop_rules::covering::Record;
    let Some(item) = item else {
        return SupersededPlan::NotFound;
    };
    if s(item, "status") != run_status::COVERED_BY_PR {
        return SupersededPlan::Nothing("This run isn't covered by another pull request.".into());
    }
    let Ok(record) = serde_json::from_str::<Record>(&s(item, "covered_by")) else {
        return SupersededPlan::Nothing("This run names no pull request it superseded.".into());
    };
    let Some(sup) = record.superseded.clone() else {
        return SupersededPlan::Nothing("This run names no pull request it superseded.".into());
    };
    if sup.closed {
        return SupersededPlan::AlreadyClosed;
    }
    // Only when the covering pull request carries the ticket's change.
    if !record.own || !matches!(record.outcome.as_str(), "continued" | "complete") {
        return SupersededPlan::Nothing(format!(
            "#{} doesn't carry this ticket's change, so #{} stays open.",
            record.number, sup.number
        ));
    }
    let Some((owner, repo)) = sup.repo.split_once('/') else {
        return SupersededPlan::Nothing("The pull request's repository isn't known.".into());
    };
    SupersededPlan::Close {
        pr: PrRef {
            owner: owner.to_string(),
            repo: repo.to_string(),
            number: sup.number,
        },
        record: Box::new(record),
        installation_id: n(item, "installation_id"),
    }
}

/// Whether one of the team's runs recorded `owner/repo#number`.
async fn run_recorded_pr(state: &AppState, team_id: &str, pr: &PrRef) -> bool {
    let repo_full = format!("{}/{}", pr.owner, pr.repo);
    let url = format!("https://github.com/{repo_full}/pull/{}", pr.number);
    let query = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("team_repo = :tr")
        .filter_expression("pr_number = :n OR contains(pr_urls, :u)")
        .expression_attribute_values(":tr", attr_s(&format!("{team_id}#{repo_full}")))
        .expression_attribute_values(":n", AttributeValue::N(pr.number.to_string()))
        .expression_attribute_values(":u", attr_s(&url));
    matches!(
        common::paged::query_first_match(query, |_| Some(())).await,
        Ok(Some(()))
    )
}

/// POST /api/runs/:run_id/close-superseded — a person closes the ticket's
/// earlier pull request a covered run superseded (CoderHelm never closes it
/// by itself: no team setting allows that). Only CoderHelm's own pull
/// request is closed, judged on GitHub's answer now
/// (`common::own_pr::judge`); a person's is refused, whatever the record
/// says.
pub async fn close_superseded(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    claims.require_role(1)?; // member+
    let item = load_run(&state, &claims.team_id, &run_id)
        .await
        .map_err(|e| {
            warn!(run_id, error = %e, "close superseded: could not read the run");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let (mut record, pr, installation_id) = match plan_close_superseded(item.as_ref()) {
        SupersededPlan::NotFound => return Err(StatusCode::NOT_FOUND),
        SupersededPlan::Nothing(why) => {
            return Ok((StatusCode::CONFLICT, Json(json!({ "message": why }))))
        }
        SupersededPlan::AlreadyClosed => {
            return Ok((
                StatusCode::OK,
                Json(json!({ "closed": true, "message": "It is closed already." })),
            ))
        }
        SupersededPlan::Close {
            record,
            pr,
            installation_id,
        } => (record, pr, installation_id),
    };
    let installation_id = if installation_id > 0 {
        installation_id
    } else {
        super::api::get_team_installation_id(&state, &claims.team_id)
            .await
            .unwrap_or(0)
    };
    let token = match crate::auth::github_app::get_installation_token(&state, installation_id).await
    {
        Ok(t) if installation_id > 0 => t,
        _ => {
            return Ok((
                StatusCode::BAD_GATEWAY,
                Json(json!({ "message": "GitHub couldn't be reached; close it there." })),
            ))
        }
    };
    let read = state
        .http
        .get(format!(
            "https://api.github.com/repos/{}/{}/pulls/{}",
            pr.owner, pr.repo, pr.number
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .send()
        .await;
    let facts = match read {
        Ok(r) if r.status().is_success() => r
            .json::<Value>()
            .await
            .ok()
            .and_then(|v| common::own_pr::PrFacts::from_github(&v)),
        _ => None,
    };
    let Some(facts) = facts else {
        return Ok((
            StatusCode::BAD_GATEWAY,
            Json(json!({ "message": "GitHub couldn't be read; close it there." })),
        ));
    };
    let bot = crate::auth::github_app::app_slug(&state)
        .await
        .map(|s| format!("{s}[bot]"));
    let recorded = run_recorded_pr(&state, &claims.team_id, &pr).await;
    if let Err(e) = common::own_pr::judge(&facts, bot.as_deref(), recorded) {
        info!(run_id, pr = pr.number, why = %e.say(pr.number), "close superseded: refused");
        return Ok((
            StatusCode::CONFLICT,
            Json(json!({ "message": format!("Not closed: {}.", e.say(pr.number)) })),
        ));
    }
    let name = profile_name(&state, &claims).await;
    let who = closer(&claims, name.as_deref());
    let comment = common::stop_rules::covering::close_comment(record.number, &who.public);
    if !close_pr_on_github(&state, &token, &pr, &comment).await {
        return Ok((
            StatusCode::BAD_GATEWAY,
            Json(json!({ "message": "GitHub didn't close the pull request; close it there." })),
        ));
    }
    if let Some(sup) = record.superseded.as_mut() {
        sup.closed = true;
        sup.closed_by = who.name.clone();
    }
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .set_key(Some(run_key(&claims.team_id, &run_id)))
        .update_expression("SET covered_by = :cb, updated_at = :t")
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_values(
            ":cb",
            attr_s(&serde_json::to_string(&record).unwrap_or_default()),
        )
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
    {
        warn!(run_id, error = %e, "close superseded: closed on GitHub, but the run record wasn't updated");
    }
    info!(run_id, pr = pr.number, "close superseded: closed");
    Ok((
        StatusCode::OK,
        Json(json!({ "closed": true, "message": format!("Closed #{}.", pr.number) })),
    ))
}

/// POST /api/runs/close — close up to [`MAX_BULK`] runs. Always 200 with one
/// result per run (in the order asked), so some can close while others
/// can't.
pub async fn close_runs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<BulkCloseBody>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    use futures::stream::{self, StreamExt};
    claims.require_role(1)?; // member+
    let ids = match bulk_ids(&body.run_ids) {
        Ok(ids) => ids,
        Err(why) => return Ok((StatusCode::BAD_REQUEST, Json(json!({ "error": why })))),
    };
    let ctx = ctx(&state, &claims, body.reason.as_deref(), body.close_pr).await;
    let ctx = &ctx;
    let results: Vec<CloseResult> = stream::iter(ids)
        .map(|id| async move { ctx.close_one(&id).await })
        .buffered(PARALLEL)
        .collect()
        .await;
    Ok((StatusCode::OK, Json(bulk_answer(&results))))
}

/// POST /api/runs/:run_id/reopen — undo a close. The run shows as it was
/// before (as waiting on a person when it was working then); no work starts.
pub async fn reopen_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    claims.require_role(1)?; // member+
    let item = load_run(&state, &claims.team_id, &run_id)
        .await
        .map_err(|e| {
            warn!(run_id, error = %e, "reopen: could not read the run");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let to = match plan_reopen(item.as_ref()) {
        ReopenPlan::NotFound => return Err(StatusCode::NOT_FOUND),
        ReopenPlan::NotClosed(status) => {
            return Ok((
                StatusCode::CONFLICT,
                Json(json!({ "status": status, "message": "It isn't closed." })),
            ))
        }
        ReopenPlan::Reopen { to } => to,
    };
    let now = chrono::Utc::now().to_rfc3339();
    let update = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .set_key(Some(run_key(&claims.team_id, &run_id)))
        .condition_expression("#s = :closed")
        .expression_attribute_names("#s", "status");
    // Reopened as waiting on a person: for what it waited on before, else
    // for a Retry by name (a run closed while it worked asked nothing).
    let update = if to == run_status::NEEDS_INPUT {
        use common::run_wait::NeedsInput;
        update
            .update_expression(
                "SET #s = :s, status_run_id = :sri, waiting_on = :wo, updated_at = :t, \
                 reopened_at = :t, reopened_by = :by \
                 REMOVE closed_at, closed_by, closed_by_name, closed_from, close_reason",
            )
            .needs_input(
                &run_id,
                &reopened_wait(item.as_ref()),
                &common::handback::HandBack::of(common::handback::HumanOnly::PersonAction),
            )
    } else {
        update
            .update_expression(
                "SET #s = :to, status_run_id = :sri, updated_at = :t, reopened_at = :t, reopened_by = :by \
                 REMOVE closed_at, closed_by, closed_by_name, closed_from, close_reason",
            )
            .expression_attribute_values(":to", attr_s(to))
            .expression_attribute_values(":sri", attr_s(&reopened_status_run_id(to, &run_id)))
    };
    let res = update
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.email))
        .expression_attribute_values(":closed", attr_s(run_status::CLOSED))
        .send()
        .await;
    match res {
        Ok(_) => {
            info!(run_id, to, "Run reopened by a person");
            Ok((
                StatusCode::OK,
                Json(
                    json!({ "status": to, "message": "Reopened. Nothing restarted; use Retry to run it again." }),
                ),
            ))
        }
        Err(e)
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
        {
            Ok((
                StatusCode::CONFLICT,
                Json(json!({ "message": "It isn't closed any more." })),
            ))
        }
        Err(e) => {
            warn!(run_id, error = %common::run_fail::error_text(&e), "reopen: write failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[cfg(test)]
mod tests {

    fn covered_item(record: serde_json::Value, status: &str) -> Item {
        HashMap::from([
            ("status".to_string(), attr_s(status)),
            ("covered_by".to_string(), attr_s(&record.to_string())),
            ("installation_id".to_string(), AttributeValue::N("7".into())),
        ])
    }

    #[test]
    fn a_superseded_pull_request_closes_only_when_the_own_covering_one_carries_the_change() {
        let rec = json!({
            "repo": "acme/web", "number": 52, "own": true, "outcome": "continued",
            "superseded": {"repo": "acme/web", "number": 41}
        });
        match plan_close_superseded(Some(&covered_item(rec.clone(), "covered_by_pr"))) {
            SupersededPlan::Close {
                pr,
                installation_id,
                ..
            } => {
                assert_eq!(
                    (pr.owner.as_str(), pr.repo.as_str(), pr.number),
                    ("acme", "web", 41)
                );
                assert_eq!(installation_id, 7);
            }
            other => panic!("{other:?}"),
        }
        let mut complete = rec.clone();
        complete["outcome"] = json!("complete");
        assert!(matches!(
            plan_close_superseded(Some(&covered_item(complete, "covered_by_pr"))),
            SupersededPlan::Close { .. }
        ));
        // A person's covering pull request: the earlier one stays open.
        let mut person = rec.clone();
        person["own"] = json!(false);
        person["outcome"] = json!("person");
        assert!(matches!(
            plan_close_superseded(Some(&covered_item(person, "covered_by_pr"))),
            SupersededPlan::Nothing(_)
        ));
        let mut closed = rec.clone();
        closed["superseded"]["closed"] = json!(true);
        assert_eq!(
            plan_close_superseded(Some(&covered_item(closed, "covered_by_pr"))),
            SupersededPlan::AlreadyClosed
        );
        let mut none = rec.clone();
        none.as_object_mut().unwrap().remove("superseded");
        assert!(matches!(
            plan_close_superseded(Some(&covered_item(none, "covered_by_pr"))),
            SupersededPlan::Nothing(_)
        ));
        assert!(matches!(
            plan_close_superseded(Some(&covered_item(rec, "completed"))),
            SupersededPlan::Nothing(_)
        ));
        assert_eq!(plan_close_superseded(None), SupersededPlan::NotFound);
    }

    /// The handler closes nothing until the pull request, read from GitHub
    /// at that moment, is CoderHelm's own: the record alone never decides.
    #[test]
    fn closing_a_superseded_pull_request_judges_it_on_github_first() {
        let src = include_str!("run_close.rs");
        let f = &src[src.find("pub async fn close_superseded(").unwrap()..];
        let judge = f.find("common::own_pr::judge(&facts").unwrap();
        let close = f.find("close_pr_on_github(&state").unwrap();
        assert!(judge < close);
    }
    use super::*;

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| {
                let val = match v.strip_prefix('#') {
                    Some(num) => AttributeValue::N(num.to_string()),
                    None => attr_s(v),
                };
                (k.to_string(), val)
            })
            .collect()
    }

    fn claims(role: &str, login: Option<&str>) -> Claims {
        Claims {
            sub: "USER#1".into(),
            team_id: "TEAM#a".into(),
            email: "sam@example.com".into(),
            role: role.into(),
            github_login: login.map(str::to_string),
            aud: "coderhelm".into(),
            exp: 0,
            iat: 0,
            support_view: None,
            custom: None,
            api_key: None,
        }
    }

    fn steps(plan: ClosePlan) -> CloseSteps {
        match plan {
            ClosePlan::Close(s) => s,
            other => panic!("expected a close, got {other:?}"),
        }
    }

    #[test]
    fn members_may_close_and_viewers_may_not() {
        assert_eq!(
            claims("viewer", None).require_role(1),
            Err(StatusCode::FORBIDDEN)
        );
        for role in ["member", "admin", "owner"] {
            assert!(claims(role, None).require_role(1).is_ok(), "{role}");
        }
        // The permissions table lists all three at member (its own test
        // checks each handler's role check against it).
        let runs = crate::permissions::CAPABILITIES
            .iter()
            .find(|c| c.key == "runs")
            .unwrap();
        for h in [
            "run_close::close_run",
            "run_close::close_runs",
            "run_close::reopen_run",
        ] {
            assert!(runs.handlers.contains(&h), "{h}");
        }
        assert_eq!(runs.min_role, "member");
    }

    #[test]
    fn every_read_and_write_is_keyed_by_the_callers_team() {
        let key = run_key("TEAM#a", "01R");
        assert_eq!(key["team_id"], attr_s("TEAM#a"));
        assert_eq!(key["run_id"], attr_s("01R"));
        // Another team's run reads as missing under this team's key.
        assert_eq!(plan_close(None, false), ClosePlan::NotFound);
        assert_eq!(plan_reopen(None), ReopenPlan::NotFound);
    }

    #[test]
    fn closing_frees_the_ticket_and_stops_auto_merge_but_keeps_the_pr() {
        let run = item(&[
            ("status", "awaiting_ci"),
            ("ticket_id", "ABC-12"),
            ("ticket_source", "jira"),
            ("repo", "acme/web"),
            ("pr_number", "#42"),
            ("pr_url", "https://github.com/acme/web/pull/42"),
            ("installation_id", "#7"),
        ]);
        let st = steps(plan_close(Some(&run), false));
        assert_eq!(st.from, "awaiting_ci");
        assert_eq!(st.ticket_lock.as_deref(), Some("ABC-12"));
        assert_eq!(
            st.prs,
            vec![PrRef {
                owner: "acme".into(),
                repo: "web".into(),
                number: 42
            }]
        );
        // The PR stays open unless asked.
        assert!(st.close_prs.is_empty());
        assert_eq!(st.jira_ticket.as_deref(), Some("ABC-12"));
        assert_eq!(st.installation_id, 7);
    }

    #[test]
    fn the_pr_closes_only_when_asked_and_only_while_open() {
        let run = item(&[
            ("status", "completed"),
            ("repo", "acme/web"),
            ("pr_number", "#42"),
        ]);
        assert_eq!(steps(plan_close(Some(&run), true)).close_prs.len(), 1);
        assert!(steps(plan_close(Some(&run), false)).close_prs.is_empty());
        let mut merged_pr = run.clone();
        merged_pr.insert("pr_outcome".into(), attr_s("closed"));
        assert!(steps(plan_close(Some(&merged_pr), true))
            .close_prs
            .is_empty());
        // No PR, nothing to close.
        let no_pr = item(&[("status", "needs_input"), ("ticket_id", "T-9")]);
        assert!(steps(plan_close(Some(&no_pr), true)).close_prs.is_empty());
    }

    #[test]
    fn a_multi_repo_run_stops_every_pr() {
        let mut run = item(&[("status", "completed"), ("repo", "acme/web")]);
        run.insert(
            "pr_urls".into(),
            AttributeValue::L(vec![
                attr_s("https://github.com/acme/web/pull/1"),
                attr_s("https://github.com/acme/api/pull/2"),
            ]),
        );
        run.insert(
            "pr_url".into(),
            attr_s("https://github.com/acme/web/pull/1"),
        );
        let st = steps(plan_close(Some(&run), true));
        assert_eq!(
            st.prs.iter().map(|p| p.number).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(st.close_prs.len(), 2);
        // A GitHub-issue run: no Jira note.
        assert_eq!(st.jira_ticket, None);
    }

    #[test]
    fn closing_twice_is_a_no_op_and_finished_runs_are_refused() {
        assert_eq!(
            plan_close(Some(&item(&[("status", "closed")])), true),
            ClosePlan::AlreadyClosed
        );
        for s in run_status::OVER {
            assert!(
                matches!(
                    plan_close(Some(&item(&[("status", s)])), false),
                    ClosePlan::Refused(_)
                ),
                "{s}"
            );
        }
    }

    #[test]
    fn the_write_only_lands_while_the_status_is_unchanged() {
        let c = closer(&claims("member", Some("sam")), Some("Sam Lee"));
        let w = close_write(
            "01R",
            "running",
            &c,
            Some("Duplicate"),
            "2026-10-23T10:00:00Z",
        );
        assert_eq!(w.condition, "attribute_exists(run_id) AND #s = :from");
        assert_eq!(w.values[":from"], attr_s("running"));
        assert_eq!(w.values[":closed"], attr_s("closed"));
        assert_eq!(w.values[":sri"], attr_s("closed#01R"));
        assert_eq!(w.values[":by"], attr_s("sam@example.com"));
        assert_eq!(w.values[":name"], attr_s("Sam Lee"));
        assert_eq!(w.values[":why"], attr_s("Duplicate"));
        assert!(w.expression.contains("closed_from = :from"));
        let w = close_write("01R", "running", &c, None, "t");
        assert!(w.expression.ends_with("REMOVE close_reason"));
        assert!(!w.values.contains_key(":why"));
    }

    #[test]
    fn the_closer_is_never_an_email_on_github_or_jira() {
        let named = closer(&claims("member", Some("sam")), Some("Sam Lee"));
        assert_eq!(
            (named.name.as_str(), named.public.as_str()),
            ("Sam Lee", "Sam Lee")
        );
        let login = closer(&claims("member", Some("sam")), None);
        assert_eq!(
            (login.name.as_str(), login.public.as_str()),
            ("sam", "@sam")
        );
        let plain = closer(&claims("member", None), None);
        assert_eq!(plain.name, "sam@example.com");
        assert_eq!(plain.public, "");
        assert!(
            run_status::ticket_note(&plain.public, None).starts_with("Closed by a team member.")
        );
    }

    #[test]
    fn reopening_restores_what_it_was_or_waits_on_a_person() {
        let closed = |from: &str| item(&[("status", "closed"), ("closed_from", from)]);
        assert_eq!(
            plan_reopen(Some(&closed("completed"))),
            ReopenPlan::Reopen { to: "completed" }
        );
        assert_eq!(
            plan_reopen(Some(&closed("failed"))),
            ReopenPlan::Reopen { to: "failed" }
        );
        assert_eq!(
            plan_reopen(Some(&closed("running"))),
            ReopenPlan::Reopen { to: "needs_input" }
        );
        assert_eq!(
            plan_reopen(Some(&item(&[("status", "closed")]))),
            ReopenPlan::Reopen { to: "needs_input" }
        );
        assert_eq!(
            plan_reopen(Some(&item(&[("status", "completed")]))),
            ReopenPlan::NotClosed("completed".into())
        );
        assert_eq!(
            reopened_status_run_id(common::run_fail::STATUS, "01R"),
            common::run_fail::status_run_id("01R")
        );
        assert_eq!(
            reopened_status_run_id("needs_input", "01R"),
            "needs_input#01R"
        );
    }

    #[test]
    fn bulk_takes_at_most_a_hundred_distinct_ids() {
        let ids = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            bulk_ids(&ids(&[" a ", "b", "a", ""])).unwrap(),
            vec!["a", "b"]
        );
        assert!(bulk_ids(&ids(&["", " "])).is_err());
        let many: Vec<String> = (0..=MAX_BULK).map(|i| format!("r{i}")).collect();
        assert!(bulk_ids(&many).is_err());
        assert_eq!(bulk_ids(&many[..MAX_BULK]).unwrap().len(), MAX_BULK);
        assert!(bulk_ids(&["x".repeat(65)]).is_err());
    }

    #[test]
    fn a_bulk_close_answers_for_every_run() {
        let mut closed = CloseResult::new("a", Outcome::Closed, "Closed.");
        closed.prs_closed = vec![4];
        let results = vec![
            closed,
            CloseResult::new("b", Outcome::AlreadyClosed, "It was closed already."),
            CloseResult::new(
                "c",
                Outcome::Refused,
                "Not closed: Its pull request merged already.",
            ),
            CloseResult::new("d", Outcome::NotFound, "No such run on this team."),
        ];
        let v = bulk_answer(&results);
        assert_eq!(v["closed"], 1);
        assert_eq!(v["already_closed"], 1);
        assert_eq!(v["failed"], 2);
        let r = v["results"].as_array().unwrap();
        assert_eq!(r.len(), 4);
        assert_eq!(r[0]["run_id"], "a");
        assert_eq!(r[0]["prs_closed"], json!([4]));
        assert_eq!(r[2]["outcome"], "refused");
        assert_eq!(r[3]["outcome"], "not_found");
        assert!(r[1].get("prs_closed").is_none());
        assert_eq!(status_of(Outcome::Refused), StatusCode::CONFLICT);
        assert_eq!(status_of(Outcome::AlreadyClosed), StatusCode::OK);
    }
}
