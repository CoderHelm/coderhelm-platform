//! The stale pull request cleanup (`common::stale_prs` has the rules).
//!
//! It rides on the review-reminder tick (EventBridge, every 20 minutes) and
//! runs each repository at most once an hour (a claim per repository and
//! hour), so a duplicate or late tick never acts twice. Per repository:
//!
//! - every open pull request is listed, oldest first, a page of 100 at a
//!   time. Only one old enough to be stale, or already carrying the stale
//!   label, has its timeline read; at most [`MAX_READS`] per sweep, carried
//!   on from where the last sweep stopped (`cursor`), so a large backlog is
//!   covered over a few hours instead of all at once;
//! - each one gets the decision from `common::stale_prs::decide`; a pull
//!   request about to be marked or closed is checked against Coderhelm's
//!   own work first (a run working on it, a draft hold that waits for
//!   Coderhelm, auto-merge waiting for CI): those are never touched;
//! - writes go one at a time with a pause between them (GitHub's guidance
//!   for its secondary limits), at most [`MAX_MARKS`] marks and the team's
//!   `max_closes_per_run` closes per sweep. GitHub's rate limit, or the
//!   installation's budget running low, stops the repository for this
//!   sweep; the next one carries on;
//! - every action is logged (`STALE_PR_LOG#`), and the plan (what is marked,
//!   what closes when, what would be marked) is saved for the dashboard
//!   (`STALE_PR_PREVIEW#`). A dry run saves only the plan.
//!
//! Closing Coderhelm's own pull request ends its run as closed, with the
//! reason on the run page, instead of leaving it waiting on a review that
//! will never come.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{DateTime, Duration, Utc};
use common::stale_prs::{self as stale, Action, Activity, Facts, Plan, PlanItem, StaleConfig};
use serde_json::Value;
use tracing::{info, warn};

use crate::clients::github::{GitHubClient, RateLimited};
use crate::models::StalePrsPreviewMessage;
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Timelines read per repository per sweep.
pub const MAX_READS: usize = 150;
/// Pull requests marked per repository per sweep.
pub const MAX_MARKS: usize = 30;
/// Pages of open pull requests read (100 each).
const MAX_PAGES: u32 = 10;
/// A timeline this long was cut off by the client; its newest events are
/// missing, so the pull request is left alone.
const TIMELINE_CAP: usize = 1000;
/// Entries kept in a saved plan.
const MAX_PLAN: usize = 200;

/// What the sweep needs from GitHub and storage, so the decisions and their
/// order can be tested without either.
#[async_trait::async_trait]
pub trait Env: Send + Sync {
    async fn open_pulls(&self) -> Result<Vec<Value>, Error>;
    async fn timeline(&self, pr: u64) -> Result<Vec<Value>, Error>;
    /// Why Coderhelm's own work holds this pull request, if it does.
    async fn held(&self, pr: &Value) -> Option<String>;
    async fn add_label(&self, pr: u64, label: &str) -> Result<(), Error>;
    async fn remove_label(&self, pr: u64, label: &str) -> Result<(), Error>;
    async fn comment(&self, pr: u64, body: &str) -> Result<(), Error>;
    async fn close(&self, pr: u64) -> Result<(), Error>;
    async fn delete_branch(&self, branch: &str) -> Result<(), Error>;
    /// Ends the Coderhelm run behind the pull request, if any; its id.
    async fn end_run(&self, pr: u64, reason: &str) -> Option<String>;
    async fn log(&self, entry: LogEntry);
    /// Once per (pull request, label time): whether to log a person taking
    /// the label off.
    async fn first_time(&self, key: &str) -> bool;
    /// The installation's budget is low enough that background work waits.
    fn budget_low(&self) -> bool;
    /// The pause between writes.
    async fn pause(&self);
}

/// One logged action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub pr: u64,
    pub action: Action,
    pub title: String,
    pub url: String,
    pub author: String,
    pub why: String,
    pub run_id: String,
}

/// One repository's sweep, as asked.
pub struct Inputs {
    pub cfg: StaleConfig,
    pub now: DateTime<Utc>,
    /// Kept-open pull requests: number and who.
    pub kept_open: HashMap<u64, String>,
    pub cursor: usize,
    /// Change things on GitHub (false: a dry run or a preview).
    pub act: bool,
}

/// What one repository's sweep did and found.
#[derive(Debug, Default)]
pub struct Outcome {
    pub items: Vec<PlanItem>,
    pub cursor: usize,
    pub read: usize,
    pub marked: usize,
    pub closed: usize,
    pub unstaled: usize,
    /// Why it stopped early (GitHub's rate limit), if it did.
    pub stopped: Option<String>,
}

fn is_rate_limit(e: &Error) -> bool {
    e.downcast_ref::<RateLimited>().is_some()
}

fn labels_of(pr: &Value) -> Vec<String> {
    pr["labels"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| l["name"].as_str().map(String::from))
        .collect()
}

fn time(v: &Value) -> Option<DateTime<Utc>> {
    v.as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))
}

/// Worth reading the timeline of: old enough to be stale, or marked.
pub fn candidate(cfg: &StaleConfig, pr: &Value, now: DateTime<Utc>) -> bool {
    let marked = labels_of(pr)
        .iter()
        .any(|l| l.eq_ignore_ascii_case(&cfg.label));
    let old =
        time(&pr["created_at"]).is_some_and(|c| now - c >= Duration::days(cfg.stale_days as i64));
    marked || old
}

/// The newest `unlabeled` of the stale label by a person, after Coderhelm's
/// own `labeled`: a person un-staled it by taking the label off.
fn removed_by_person(timeline: &[Value], label: &str) -> Option<(DateTime<Utc>, String)> {
    let ours = timeline
        .iter()
        .filter(|e| {
            e["event"].as_str() == Some("labeled")
                && e["label"]["name"]
                    .as_str()
                    .is_some_and(|n| n.eq_ignore_ascii_case(label))
                && stale::is_own(e["actor"]["login"].as_str().unwrap_or(""))
        })
        .filter_map(|e| time(&e["created_at"]))
        .max()?;
    timeline
        .iter()
        .filter(|e| {
            e["event"].as_str() == Some("unlabeled")
                && e["label"]["name"]
                    .as_str()
                    .is_some_and(|n| n.eq_ignore_ascii_case(label))
        })
        .filter_map(|e| {
            let at = time(&e["created_at"])?;
            let who = e["actor"]["login"].as_str().unwrap_or("").to_string();
            (at > ours && !stale::is_own(&who)).then_some((at, who))
        })
        .max_by_key(|(at, _)| *at)
}

fn item(
    pr: &Value,
    plan: &str,
    why: &str,
    last: &Activity,
    next_at: Option<DateTime<Utc>>,
) -> PlanItem {
    PlanItem {
        pr: pr["number"].as_u64().unwrap_or(0),
        title: pr["title"].as_str().unwrap_or("").to_string(),
        author: pr["user"]["login"].as_str().unwrap_or("").to_string(),
        url: pr["html_url"].as_str().unwrap_or("").to_string(),
        action: plan.to_string(),
        why: why.to_string(),
        last_activity: last.at.to_rfc3339(),
        last_what: last.what.clone(),
        next_at: next_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
    }
}

fn entry(pr: &Value, action: Action, why: &str) -> LogEntry {
    LogEntry {
        pr: pr["number"].as_u64().unwrap_or(0),
        action,
        title: pr["title"].as_str().unwrap_or("").to_string(),
        url: pr["html_url"].as_str().unwrap_or("").to_string(),
        author: pr["user"]["login"].as_str().unwrap_or("").to_string(),
        why: why.to_string(),
        run_id: String::new(),
    }
}

/// One repository's sweep: the plan, and (when `act`) the writes.
pub async fn run_repo(env: &dyn Env, inp: &Inputs) -> Result<Outcome, Error> {
    let cfg = &inp.cfg;
    let mut out = Outcome::default();
    let pulls = env.open_pulls().await?;
    let candidates: Vec<&Value> = pulls
        .iter()
        .filter(|p| candidate(cfg, p, inp.now))
        .collect();
    if candidates.is_empty() {
        return Ok(out);
    }
    let start = inp.cursor % candidates.len();
    let mut rotated = candidates[start..].to_vec();
    rotated.extend_from_slice(&candidates[..start]);

    let stop = |why: &str, out: &mut Outcome| {
        out.stopped = Some(why.to_string());
    };
    for pr in rotated.into_iter().take(MAX_READS) {
        if env.budget_low() {
            stop(
                "the GitHub budget is low; the rest wait for the next sweep",
                &mut out,
            );
            break;
        }
        let number = pr["number"].as_u64().unwrap_or(0);
        if number == 0 {
            continue;
        }
        out.read += 1;
        let timeline = match env.timeline(number).await {
            Ok(t) => t,
            Err(e) if is_rate_limit(&e) => {
                out.read -= 1;
                stop("GitHub's rate limit", &mut out);
                break;
            }
            Err(e) => {
                warn!(pr = number, error = %e, "stale PRs: could not read the timeline");
                continue;
            }
        };
        if timeline.len() >= TIMELINE_CAP {
            info!(
                pr = number,
                "stale PRs: timeline too long to read in full, left alone"
            );
            continue;
        }
        let labels = labels_of(pr);
        let label_on = labels.iter().any(|l| l.eq_ignore_ascii_case(&cfg.label));
        let last = stale::last_activity(pr, &timeline, &cfg.label);
        let marked = label_on.then(|| {
            // A label with no `labeled` event read (the record is gone):
            // the grace period starts now.
            stale::marked(&timeline, &cfg.label).unwrap_or((inp.now, false))
        });
        let author = pr["user"]["login"].as_str().unwrap_or("");
        let base_exempt = stale::Exemption {
            cfg,
            draft: pr["draft"].as_bool().unwrap_or(false),
            labels: &labels,
            author,
            kept_open_by: inp.kept_open.get(&number).map(String::as_str),
            held: None,
        }
        .reason();
        let mut facts = Facts {
            now: inp.now,
            last: last.clone(),
            marked,
            exempt: base_exempt,
        };
        let mut plan = stale::decide(cfg, &facts);
        if matches!(plan, Plan::Mark | Plan::Close) {
            if let Some(why) = env.held(pr).await {
                facts.exempt = Some(why);
                plan = stale::decide(cfg, &facts);
            }
        }
        let quiet = inp.now - last.at >= Duration::days(cfg.stale_days as i64);

        // A person took Coderhelm's label off: logged once.
        if !label_on && inp.act {
            if let Some((at, who)) = removed_by_person(&timeline, &cfg.label) {
                let key = format!("{number}#{}", at.timestamp());
                if env.first_time(&key).await {
                    out.unstaled += 1;
                    env.log(entry(
                        pr,
                        Action::Unstaled,
                        &format!("{who} removed the {} label", cfg.label),
                    ))
                    .await;
                }
            }
        }

        match &plan {
            Plan::Leave { why } => {
                if quiet && facts.exempt.is_some() {
                    out.items.push(item(pr, "exempt", why, &last, None));
                }
            }
            Plan::Wait { closes_at } => {
                out.items
                    .push(item(pr, "wait", "marked stale", &last, Some(*closes_at)));
            }
            Plan::Mark => {
                let closes_at = inp.now + Duration::days(cfg.close_days as i64);
                let why = format!("no activity since {}", last.at.format("%b %-d"));
                if !inp.act {
                    out.items
                        .push(item(pr, "mark", &why, &last, Some(closes_at)));
                    continue;
                }
                if out.marked >= MAX_MARKS {
                    out.items
                        .push(item(pr, "mark", "waits for the next sweep", &last, None));
                    continue;
                }
                match env.add_label(number, &cfg.label).await {
                    Ok(()) => {}
                    Err(e) if is_rate_limit(&e) => {
                        stop("GitHub's rate limit", &mut out);
                        break;
                    }
                    Err(e) => {
                        warn!(pr = number, error = %e, "stale PRs: could not add the label");
                        continue;
                    }
                }
                env.pause().await;
                if let Err(e) = env.comment(number, &cfg.warn_comment()).await {
                    warn!(pr = number, error = %e, "stale PRs: the warning comment failed");
                }
                env.pause().await;
                out.marked += 1;
                env.log(entry(pr, Action::Marked, &why)).await;
                out.items
                    .push(item(pr, "wait", "marked stale", &last, Some(closes_at)));
            }
            Plan::Unmark { why } => {
                if !inp.act {
                    out.items.push(item(pr, "unmark", why, &last, None));
                    continue;
                }
                match env.remove_label(number, &cfg.label).await {
                    Ok(()) => {
                        env.pause().await;
                        out.unstaled += 1;
                        env.log(entry(pr, Action::Unstaled, why)).await;
                    }
                    Err(e) if is_rate_limit(&e) => {
                        stop("GitHub's rate limit", &mut out);
                        break;
                    }
                    Err(e) => {
                        warn!(pr = number, error = %e, "stale PRs: could not take the label off")
                    }
                }
            }
            Plan::Close => {
                let why = format!(
                    "marked stale {} days ago with no activity since",
                    cfg.close_days
                );
                if !inp.act {
                    out.items
                        .push(item(pr, "close", &why, &last, Some(inp.now)));
                    continue;
                }
                if out.closed >= cfg.max_closes_per_run as usize {
                    out.items.push(item(
                        pr,
                        "close",
                        "waits for the next sweep (closes per sweep reached)",
                        &last,
                        None,
                    ));
                    continue;
                }
                match env.close(number).await {
                    Ok(()) => {}
                    Err(e) if is_rate_limit(&e) => {
                        stop("GitHub's rate limit", &mut out);
                        break;
                    }
                    Err(e) => {
                        warn!(pr = number, error = %e, "stale PRs: could not close");
                        continue;
                    }
                }
                env.pause().await;
                let mut deleted = false;
                if stale::may_delete_branch(cfg, pr, &pulls) {
                    let branch = pr["head"]["ref"].as_str().unwrap_or("");
                    match env.delete_branch(branch).await {
                        Ok(()) => deleted = true,
                        Err(e) => {
                            warn!(pr = number, branch, error = %e, "stale PRs: branch not deleted")
                        }
                    }
                    env.pause().await;
                }
                if let Err(e) = env.comment(number, &cfg.close_comment(deleted)).await {
                    warn!(pr = number, error = %e, "stale PRs: the closing comment failed");
                }
                env.pause().await;
                let reason = stale::run_close_reason(number, cfg.stale_days, cfg.close_days);
                let run_id = env.end_run(number, &reason).await.unwrap_or_default();
                out.closed += 1;
                let mut e = entry(pr, Action::Closed, &why);
                if deleted {
                    e.why.push_str("; branch deleted");
                }
                e.run_id = run_id;
                env.log(e).await;
            }
        }
    }
    out.cursor = if out.stopped.is_some() || candidates.len() <= MAX_READS {
        (start + out.read) % candidates.len()
    } else {
        (start + MAX_READS) % candidates.len()
    };
    out.items.truncate(MAX_PLAN);
    Ok(out)
}

// ── The real environment ────────────────────────────────────────────

struct Live<'a> {
    state: &'a WorkerState,
    github: GitHubClient,
    team_id: &'a str,
    owner: &'a str,
    name: &'a str,
}

impl Live<'_> {
    fn repo(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }
}

#[async_trait::async_trait]
impl Env for Live<'_> {
    async fn open_pulls(&self) -> Result<Vec<Value>, Error> {
        self.github
            .list_open_pulls_all(self.owner, self.name, MAX_PAGES)
            .await
    }

    async fn timeline(&self, pr: u64) -> Result<Vec<Value>, Error> {
        let v = self
            .github
            .list_issue_timeline(self.owner, self.name, pr)
            .await?;
        Ok(v.as_array().cloned().unwrap_or_default())
    }

    async fn held(&self, pr: &Value) -> Option<String> {
        let number = pr["number"].as_u64()?;
        held_by_coderhelm(self.state, self.team_id, self.owner, self.name, number).await
    }

    async fn add_label(&self, pr: u64, label: &str) -> Result<(), Error> {
        self.github
            .add_labels(self.owner, self.name, pr, &[label.to_string()])
            .await
            .map(|_| ())
    }

    async fn remove_label(&self, pr: u64, label: &str) -> Result<(), Error> {
        self.github
            .remove_label(self.owner, self.name, pr, label)
            .await
    }

    async fn comment(&self, pr: u64, body: &str) -> Result<(), Error> {
        self.github
            .create_issue_comment(self.owner, self.name, pr, body)
            .await
            .map(|_| ())
    }

    async fn close(&self, pr: u64) -> Result<(), Error> {
        self.github
            .close_pull_request(self.owner, self.name, pr)
            .await
            .map(|_| ())
    }

    async fn delete_branch(&self, branch: &str) -> Result<(), Error> {
        self.github
            .delete_branch(self.owner, self.name, branch)
            .await
    }

    async fn end_run(&self, pr: u64, reason: &str) -> Option<String> {
        end_run(self.state, self.team_id, &self.repo(), pr, reason).await
    }

    async fn log(&self, e: LogEntry) {
        write_log(self.state, self.team_id, &self.repo(), &e).await;
    }

    async fn first_time(&self, key: &str) -> bool {
        super::review_pr::claim_once_ttl(
            self.state,
            self.team_id,
            &format!("STALE_PR_UNLABELED#{}#{key}", self.repo()),
            30 * 86_400,
        )
        .await
    }

    fn budget_low(&self) -> bool {
        self.github.background_wait().is_some()
    }

    async fn pause(&self) {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// Why Coderhelm's own work holds the pull request: its newest run is still
/// working on it, its draft hold waits for Coderhelm to check again, or its
/// auto-merge waits for CI. Any lookup that fails holds it (never close what
/// can't be checked).
pub(crate) async fn held_by_coderhelm(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
    pr: u64,
) -> Option<String> {
    let repo = format!("{owner}/{name}");
    let gate = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key(
            "sk",
            attr_s(&common::merge_gate::record_sk(owner, name, pr)),
        )
        .send()
        .await;
    match gate {
        Ok(o) => {
            let st = o
                .item()
                .and_then(|i| i.get("state"))
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            if st == common::merge_gate::STATE_WAITING_CI {
                return Some("waiting for CI to merge it (auto-merge)".to_string());
            }
        }
        Err(_) => return Some("its auto-merge state couldn't be read".to_string()),
    }
    let runs = super::covering::runs_for_pr(state, team_id, &repo, pr).await;
    let run_id = runs.first()?;
    let item = match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .consistent_read(true)
        .send()
        .await
    {
        Ok(o) => o.item().cloned()?,
        Err(_) => return Some("its Coderhelm run couldn't be read".to_string()),
    };
    hold_of_run(run_id, &item, Utc::now())
}

/// Pure: why a run's record holds its pull request open.
pub fn hold_of_run(
    run_id: &str,
    item: &HashMap<String, AttributeValue>,
    now: DateTime<Utc>,
) -> Option<String> {
    let holder = super::pr_holder::Holder::from_item(run_id, item);
    if let Some(why) = super::pr_holder::busy(&holder, now, "") {
        return Some(format!("a Coderhelm run is working on it ({why})"));
    }
    if let Some(h) = common::draft_hold::DraftHold::of_item(item) {
        if h.kind == common::draft_hold::Kind::Waiting {
            let what = h
                .reasons
                .first()
                .map(|r| r.what.clone())
                .unwrap_or_default();
            return Some(format!("Coderhelm checks it again on its own ({what})"));
        }
    }
    None
}

/// Ends the newest run behind the pull request as closed, with the reason,
/// when it is still open (not already over, not working: working runs are
/// never closed here). Its id when ended.
async fn end_run(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    pr: u64,
    reason: &str,
) -> Option<String> {
    let runs = super::covering::runs_for_pr(state, team_id, repo, pr).await;
    let run_id = runs.first()?.clone();
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(&run_id))
        .consistent_read(true)
        .send()
        .await
        .ok()?
        .item()
        .cloned()?;
    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    if !run_closes(&status) {
        return None;
    }
    let now = Utc::now().to_rfc3339();
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(&run_id))
        .update_expression(
            "SET #s = :closed, status_run_id = :sri, updated_at = :t, closed_at = :t, \
             closed_by = :by, closed_by_name = :name, closed_from = :from, close_reason = :why",
        )
        .condition_expression("attribute_exists(run_id) AND #s = :from")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":closed", attr_s(common::run_status::CLOSED))
        .expression_attribute_values(
            ":sri",
            attr_s(&format!("{}#{run_id}", common::run_status::CLOSED)),
        )
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":by", attr_s("coderhelm"))
        .expression_attribute_values(":name", attr_s("Coderhelm"))
        .expression_attribute_values(":from", attr_s(&status))
        .expression_attribute_values(":why", attr_s(reason))
        .send()
        .await;
    match res {
        Ok(_) => {
            if let Some(ticket) = item.get("ticket_id").and_then(|v| v.as_s().ok()) {
                if !ticket.is_empty() {
                    super::release_ticket_lock(state, team_id, ticket).await;
                }
            }
            info!(
                team_id,
                run_id, pr, "stale PRs: the run behind the closed pull request ended as closed"
            );
            Some(run_id)
        }
        Err(e) => {
            warn!(team_id, run_id, pr, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "stale PRs: the run could not be ended");
            None
        }
    }
}

/// Pure: a run in `status` is ended when its pull request closes as stale:
/// open and not working (working runs hold the pull request open).
pub fn run_closes(status: &str) -> bool {
    use common::run_status::{self as rs, CloseCheck};
    rs::close_check(status) == CloseCheck::Close && !rs::IN_FLIGHT.contains(&status)
}

async fn write_log(state: &WorkerState, team_id: &str, repo: &str, e: &LogEntry) {
    let now = Utc::now();
    if let Err(err) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&stale::log_sk(&now, repo, e.pr)))
        .item("repo", attr_s(repo))
        .item("pr_number", attr_n(e.pr))
        .item("action", attr_s(e.action.as_str()))
        .item("title", attr_s(common::truncate_str(&e.title, 300)))
        .item("url", attr_s(&e.url))
        .item("author", attr_s(&e.author))
        .item("why", attr_s(common::truncate_str(&e.why, 500)))
        .item("run_id", attr_s(&e.run_id))
        .item("by", attr_s("Coderhelm"))
        .item("at", attr_s(&now.to_rfc3339()))
        .item(
            "ttl",
            attr_n(now.timestamp() + stale::LOG_TTL_DAYS * 86_400),
        )
        .send()
        .await
    {
        warn!(repo, pr = e.pr, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&err), "stale PRs: could not log the action");
    }
    info!(team_id, repo, pr = e.pr, action = e.action.as_str(), why = %e.why, "stale_pr_action");
}

// ── Settings and records ────────────────────────────────────────────

/// A team's settings: the team's own and each repository's.
#[derive(Default)]
struct TeamSettings {
    team: Option<StaleConfig>,
    repos: HashMap<String, StaleConfig>,
}

impl TeamSettings {
    fn for_repo(&self, repo: &str) -> StaleConfig {
        let r = self
            .repos
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(repo))
            .map(|(_, v)| v);
        stale::effective(self.team.as_ref(), r)
    }
}

async fn team_settings(state: &WorkerState, team_id: &str) -> TeamSettings {
    let mut out = TeamSettings::default();
    let mut start = None;
    for _ in 0..20 {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
            .expression_attribute_values(":p", attr_s(team_id))
            .expression_attribute_values(":pfx", attr_s(stale::CONFIG_PREFIX))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(resp) = resp else {
            warn!(team_id, "stale PRs: settings could not be read");
            break;
        };
        for i in resp.items() {
            let sk = i
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            let Some(cfg) = i
                .get(stale::ATTR_CONFIG)
                .and_then(|v| v.as_s().ok())
                .and_then(|s| StaleConfig::from_json(s))
            else {
                continue;
            };
            match stale::repo_of_sk(&sk) {
                Some(repo) => {
                    out.repos.insert(repo.to_string(), cfg);
                }
                None if sk == stale::TEAM_SK => out.team = Some(cfg),
                None => {}
            }
        }
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out
}

async fn kept_open(state: &WorkerState, team_id: &str, repo: &str) -> HashMap<u64, String> {
    let mut out = HashMap::new();
    let mut start = None;
    for _ in 0..20 {
        let Ok(resp) = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
            .expression_attribute_values(":p", attr_s(team_id))
            .expression_attribute_values(":pfx", attr_s(&stale::exempt_prefix(repo)))
            .set_exclusive_start_key(start.take())
            .send()
            .await
        else {
            break;
        };
        for i in resp.items() {
            let n = i
                .get("pr_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok());
            let by = i
                .get("by")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            if let Some(n) = n {
                out.insert(n, by);
            }
        }
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out
}

async fn saved_cursor(state: &WorkerState, team_id: &str, repo: &str) -> usize {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&stale::preview_sk(repo)))
        .projection_expression("#c")
        .expression_attribute_names("#c", "cursor")
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .and_then(|i| {
            i.get("cursor")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
        })
        .unwrap_or(0)
}

async fn save_plan(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    cfg: &StaleConfig,
    out: &Outcome,
    preview: bool,
) {
    let now = Utc::now();
    let plan = serde_json::to_string(&out.items).unwrap_or_else(|_| "[]".into());
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&stale::preview_sk(repo)))
        .item("repo", attr_s(repo))
        .item("plan", attr_s(&plan))
        .item("computed_at", attr_s(&now.to_rfc3339()))
        .item(
            "dry_run",
            AttributeValue::Bool(!cfg.enabled || cfg.dry_run || preview),
        )
        .item("enabled", AttributeValue::Bool(cfg.enabled))
        .item("cursor", attr_n(out.cursor))
        .item("read", attr_n(out.read))
        .item("stopped", attr_s(out.stopped.as_deref().unwrap_or("")))
        .send()
        .await
    {
        warn!(repo, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "stale PRs: could not save the plan");
    }
}

/// Whether a reviewer config covers the repository: the reviewer on and not
/// stopped.
fn reviewed(item: &HashMap<String, AttributeValue>) -> bool {
    let flag = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false)
    };
    flag("enabled") && !flag("killed")
}

/// What one scheduled sweep did.
#[derive(Debug, Default)]
pub struct SweepReport {
    pub repos: usize,
    pub marked: usize,
    pub closed: usize,
    pub unstaled: usize,
}

/// The scheduled sweep over every repository with the reviewer on, for the
/// teams that turned the cleanup on.
pub async fn sweep(state: &WorkerState) -> SweepReport {
    let mut report = SweepReport::default();
    let configs = super::review_configs::all(state, "stale-prs").await;
    let mut settings: HashMap<String, TeamSettings> = HashMap::new();
    let mut installs: HashMap<String, Option<u64>> = HashMap::new();
    let hour = Utc::now().format("%Y%m%d%H").to_string();
    for c in configs {
        if !reviewed(&c.item) {
            continue;
        }
        if !settings.contains_key(&c.team_id) {
            let s = team_settings(state, &c.team_id).await;
            settings.insert(c.team_id.clone(), s);
        }
        let cfg = settings[&c.team_id].for_repo(&c.repo);
        if !cfg.enabled {
            continue;
        }
        let claim = format!("STALE_PR_SWEEP#{}#{hour}", c.repo);
        if !super::review_pr::claim_once_ttl(state, &c.team_id, &claim, 2 * 3600).await {
            continue;
        }
        let iid = match installs.get(&c.team_id) {
            Some(v) => *v,
            None => {
                let v = super::review_reminder::team_installation_id(state, &c.team_id).await;
                installs.insert(c.team_id.clone(), v);
                v
            }
        };
        let Some(iid) = iid else {
            continue;
        };
        report.repos += 1;
        match sweep_one(state, &c.team_id, &c.repo, iid, cfg, false).await {
            Ok(o) => {
                report.marked += o.marked;
                report.closed += o.closed;
                report.unstaled += o.unstaled;
            }
            Err(e) => warn!(repo = %c.repo, error = %e, "stale PRs: sweep failed for repo"),
        }
    }
    report
}

/// The dashboard's preview: one repository's plan now, nothing changed.
pub async fn preview(state: &WorkerState, msg: &StalePrsPreviewMessage) {
    let s = team_settings(state, &msg.team_id).await;
    let cfg = s.for_repo(&msg.repo);
    let Some(iid) = super::review_reminder::team_installation_id(state, &msg.team_id).await else {
        warn!(team_id = %msg.team_id, "stale PRs: no GitHub installation for the preview");
        return;
    };
    if let Err(e) = sweep_one(state, &msg.team_id, &msg.repo, iid, cfg, true).await {
        warn!(repo = %msg.repo, error = %e, "stale PRs: preview failed");
    }
}

async fn sweep_one(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    installation_id: u64,
    cfg: StaleConfig,
    preview: bool,
) -> Result<Outcome, Error> {
    let (owner, name) = repo.split_once('/').ok_or("bad repo slug")?;
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        installation_id,
        &state.http,
    )?;
    let env = Live {
        state,
        github,
        team_id,
        owner,
        name,
    };
    let inputs = Inputs {
        act: cfg.enabled && !cfg.dry_run && !preview,
        kept_open: kept_open(state, team_id, repo).await,
        cursor: if preview {
            0
        } else {
            saved_cursor(state, team_id, repo).await
        },
        now: Utc::now(),
        cfg,
    };
    let out = run_repo(&env, &inputs).await?;
    save_plan(state, team_id, repo, &inputs.cfg, &out, preview).await;
    info!(
        team_id,
        repo,
        act = inputs.act,
        read = out.read,
        marked = out.marked,
        closed = out.closed,
        unstaled = out.unstaled,
        planned = out.items.len(),
        stopped = out.stopped.as_deref().unwrap_or(""),
        "Stale PR sweep done for repo"
    );
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Fake {
        pulls: Vec<Value>,
        timelines: HashMap<u64, Vec<Value>>,
        held: HashMap<u64, String>,
        writes: Mutex<Vec<String>>,
        logs: Mutex<Vec<LogEntry>>,
        reads: Mutex<usize>,
        /// The timeline read that answers with GitHub's rate limit.
        limit_at: Option<usize>,
        budget_low: bool,
    }

    impl Fake {
        fn writes(&self) -> Vec<String> {
            self.writes.lock().unwrap().clone()
        }
        fn w(&self, s: String) -> Result<(), Error> {
            self.writes.lock().unwrap().push(s);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Env for Fake {
        async fn open_pulls(&self) -> Result<Vec<Value>, Error> {
            Ok(self.pulls.clone())
        }
        async fn timeline(&self, pr: u64) -> Result<Vec<Value>, Error> {
            let mut n = self.reads.lock().unwrap();
            *n += 1;
            if self.limit_at == Some(*n) {
                return Err(Box::new(crate::clients::github::RateLimited::for_test(60)));
            }
            Ok(self.timelines.get(&pr).cloned().unwrap_or_default())
        }
        async fn held(&self, pr: &Value) -> Option<String> {
            self.held.get(&pr["number"].as_u64().unwrap()).cloned()
        }
        async fn add_label(&self, pr: u64, label: &str) -> Result<(), Error> {
            self.w(format!("label #{pr} {label}"))
        }
        async fn remove_label(&self, pr: u64, label: &str) -> Result<(), Error> {
            self.w(format!("unlabel #{pr} {label}"))
        }
        async fn comment(&self, pr: u64, body: &str) -> Result<(), Error> {
            assert!(body.contains(stale::MARKER));
            self.w(format!("comment #{pr}"))
        }
        async fn close(&self, pr: u64) -> Result<(), Error> {
            self.w(format!("close #{pr}"))
        }
        async fn delete_branch(&self, branch: &str) -> Result<(), Error> {
            self.w(format!("delete {branch}"))
        }
        async fn end_run(&self, pr: u64, _reason: &str) -> Option<String> {
            (pr == 3).then(|| "run-3".to_string())
        }
        async fn log(&self, e: LogEntry) {
            self.logs.lock().unwrap().push(e);
        }
        async fn first_time(&self, _key: &str) -> bool {
            true
        }
        fn budget_low(&self) -> bool {
            self.budget_low
        }
        async fn pause(&self) {}
    }

    fn now() -> DateTime<Utc> {
        "2026-11-10T12:00:00Z".parse().unwrap()
    }

    fn pr(n: u64, created: &str, labels: &[&str], author: &str) -> Value {
        json!({
            "number": n, "title": format!("PR {n}"), "html_url": format!("https://github.com/acme/web/pull/{n}"),
            "created_at": created, "draft": false, "user": {"login": author, "type": "User"},
            "labels": labels.iter().map(|l| json!({"name": l})).collect::<Vec<_>>(),
            "head": {"ref": format!("feat-{n}"), "repo": {"full_name": "acme/web", "fork": false}},
            "base": {"ref": "main", "repo": {"full_name": "acme/web", "default_branch": "main"}}
        })
    }

    fn our_label(at: &str) -> Value {
        json!({"event": "labeled", "actor": {"login": "coderhelm[bot]", "type": "Bot"}, "label": {"name": "stale"}, "created_at": at})
    }

    fn inputs(act: bool) -> Inputs {
        Inputs {
            cfg: StaleConfig {
                enabled: true,
                dry_run: !act,
                ..Default::default()
            },
            now: now(),
            kept_open: HashMap::new(),
            cursor: 0,
            act,
        }
    }

    /// #1 quiet 40 days: marked. #2 new: left. #3 marked 8 days ago and
    /// quiet: closed (its run ended). #4 marked, then a person commented:
    /// un-staled. #5 quiet but a Coderhelm run works on it: left.
    fn repo() -> Fake {
        let mut f = Fake {
            pulls: vec![
                pr(1, "2026-10-01T00:00:00Z", &[], "ana"),
                pr(2, "2026-11-09T00:00:00Z", &[], "ana"),
                pr(3, "2026-09-01T00:00:00Z", &["stale"], "coderhelm[bot]"),
                pr(4, "2026-09-01T00:00:00Z", &["stale"], "bo"),
                pr(5, "2026-09-01T00:00:00Z", &[], "coderhelm[bot]"),
            ],
            ..Default::default()
        };
        f.timelines
            .insert(3, vec![our_label("2026-11-02T00:00:00Z")]);
        f.timelines.insert(
            4,
            vec![
                our_label("2026-11-01T00:00:00Z"),
                json!({"event": "commented", "actor": {"login": "bo", "type": "User"}, "created_at": "2026-11-05T00:00:00Z"}),
            ],
        );
        f.held.insert(5, "a Coderhelm run is working on it".into());
        f
    }

    #[tokio::test]
    async fn a_sweep_marks_closes_and_unstales_in_one_pass() {
        let f = repo();
        let out = run_repo(&f, &inputs(true)).await.unwrap();
        assert_eq!(
            f.writes(),
            vec![
                "label #1 stale",
                "comment #1",
                "close #3",
                "comment #3",
                "unlabel #4 stale"
            ]
        );
        assert_eq!((out.marked, out.closed, out.unstaled), (1, 1, 1));
        // #2 is too new to read at all.
        assert_eq!(out.read, 4);
        let logs = f.logs.lock().unwrap().clone();
        let closed = logs.iter().find(|l| l.action == Action::Closed).unwrap();
        assert_eq!((closed.pr, closed.run_id.as_str()), (3, "run-3"));
        assert!(logs.iter().all(|l| l.pr != 5));
        // The plan shows #1 waiting to close and #5 kept open.
        let wait = out.items.iter().find(|i| i.pr == 1).unwrap();
        assert_eq!(wait.action, "wait");
        assert_eq!(
            out.items.iter().find(|i| i.pr == 5).unwrap().action,
            "exempt"
        );
    }

    #[tokio::test]
    async fn a_dry_run_writes_nothing_and_says_what_it_would_do() {
        let f = repo();
        let out = run_repo(&f, &inputs(false)).await.unwrap();
        assert!(f.writes().is_empty());
        assert!(f.logs.lock().unwrap().is_empty());
        let action = |n: u64| {
            out.items
                .iter()
                .find(|i| i.pr == n)
                .map(|i| i.action.clone())
        };
        assert_eq!(action(1).as_deref(), Some("mark"));
        assert_eq!(action(3).as_deref(), Some("close"));
        assert_eq!(action(4).as_deref(), Some("unmark"));
        assert_eq!(action(2), None);
    }

    #[tokio::test]
    async fn kept_open_and_held_pull_requests_are_never_closed() {
        let mut f = repo();
        f.held
            .insert(3, "Coderhelm checks it again on its own".into());
        let mut inp = inputs(true);
        inp.kept_open.insert(1, "ana".into());
        let out = run_repo(&f, &inp).await.unwrap();
        let w = f.writes();
        assert!(
            !w.iter().any(|s| s == "close #3" || s == "label #1 stale"),
            "{w:?}"
        );
        // Coderhelm's own label comes off the held one.
        assert!(w.contains(&"unlabel #3 stale".to_string()));
        assert_eq!(out.closed, 0);
    }

    #[tokio::test]
    async fn closes_stop_at_the_cap_and_branches_stay_unless_asked() {
        let mut f = Fake::default();
        for n in 10..15 {
            f.pulls
                .push(pr(n, "2026-09-01T00:00:00Z", &["stale"], "ana"));
            f.timelines
                .insert(n, vec![our_label("2026-10-01T00:00:00Z")]);
        }
        let mut inp = inputs(true);
        inp.cfg.max_closes_per_run = 2;
        let out = run_repo(&f, &inp).await.unwrap();
        assert_eq!(out.closed, 2);
        assert!(!f.writes().iter().any(|w| w.starts_with("delete")));
        assert_eq!(out.items.iter().filter(|i| i.action == "close").count(), 3);

        let f2 = Fake {
            pulls: f.pulls.clone(),
            timelines: f.timelines.clone(),
            ..Default::default()
        };
        inp.cfg.delete_branch = true;
        run_repo(&f2, &inp).await.unwrap();
        assert!(f2.writes().contains(&"delete feat-10".to_string()));
    }

    #[tokio::test]
    async fn a_rate_limit_stops_the_repo_and_the_next_sweep_carries_on() {
        let mut f = Fake {
            limit_at: Some(3),
            ..Default::default()
        };
        for n in 1..=6 {
            f.pulls.push(pr(n, "2026-11-05T00:00:00Z", &[], "ana"));
        }
        let mut inp = inputs(true);
        inp.cfg.stale_days = 1;
        let out = run_repo(&f, &inp).await.unwrap();
        assert_eq!(out.stopped.as_deref(), Some("GitHub's rate limit"));
        assert_eq!(out.read, 2);
        assert_eq!(out.cursor, 2);
        // The next sweep starts at the third.
        let f2 = Fake {
            pulls: f.pulls.clone(),
            ..Default::default()
        };
        inp.cursor = out.cursor;
        let out2 = run_repo(&f2, &inp).await.unwrap();
        assert_eq!(
            f2.writes().first().map(String::as_str),
            Some("label #3 stale")
        );
        assert_eq!(out2.read, 6);

        let low = Fake {
            pulls: f.pulls.clone(),
            budget_low: true,
            ..Default::default()
        };
        let out3 = run_repo(&low, &inp).await.unwrap();
        assert_eq!(out3.read, 0);
        assert!(low.writes().is_empty());
    }

    #[tokio::test]
    async fn reads_are_bounded_per_sweep_and_rotate() {
        let mut f = Fake::default();
        for n in 1..=(MAX_READS as u64 + 50) {
            f.pulls.push(pr(n, "2026-11-01T00:00:00Z", &[], "ana"));
        }
        let mut inp = inputs(false);
        inp.cfg.stale_days = 100;
        // Nothing is old enough: nothing read.
        assert_eq!(run_repo(&f, &inp).await.unwrap().read, 0);
        inp.cfg.stale_days = 1;
        let out = run_repo(&f, &inp).await.unwrap();
        assert_eq!(out.read, MAX_READS);
        assert_eq!(out.cursor, MAX_READS);
        inp.cursor = out.cursor;
        let out = run_repo(&f, &inp).await.unwrap();
        assert_eq!(out.cursor, (2 * MAX_READS) % (MAX_READS + 50));
    }

    #[tokio::test]
    async fn a_person_removing_the_label_is_logged_as_unstaled() {
        let mut f = Fake {
            pulls: vec![pr(8, "2026-09-01T00:00:00Z", &[], "ana")],
            ..Default::default()
        };
        f.timelines.insert(
            8,
            vec![
                our_label("2026-11-01T00:00:00Z"),
                json!({"event": "unlabeled", "actor": {"login": "ana", "type": "User"}, "label": {"name": "stale"}, "created_at": "2026-11-03T00:00:00Z"}),
            ],
        );
        let out = run_repo(&f, &inputs(true)).await.unwrap();
        assert!(
            f.writes().is_empty(),
            "the label is off and the removal is recent: nothing to do"
        );
        assert_eq!(out.unstaled, 1);
        let logs = f.logs.lock().unwrap().clone();
        assert_eq!(logs[0].action, Action::Unstaled);
        assert!(logs[0].why.contains("ana removed"));
    }

    #[test]
    fn a_working_or_waiting_run_holds_its_pull_request() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("status".into(), attr_s("running"));
        assert!(hold_of_run("r", &item, now())
            .unwrap()
            .contains("working on it"));
        item.insert("status".into(), attr_s("needs_input"));
        assert_eq!(hold_of_run("r", &item, now()), None);
        let hold = common::draft_hold::DraftHold::new(
            common::draft_hold::Kind::Waiting,
            vec![common::draft_hold::Reason::new(
                "checks are still running",
                "",
            )],
            7,
            "",
        )
        .unwrap();
        item.insert(common::draft_hold::FIELD.into(), hold.to_attr());
        let got = hold_of_run("r", &item, now());
        assert!(
            got.as_deref()
                .is_some_and(|g| g.contains("checks it again on its own")),
            "{got:?}"
        );
        // A hold that waits on a person doesn't keep it open.
        let person = common::draft_hold::DraftHold::new(
            common::draft_hold::Kind::Held,
            vec![common::draft_hold::Reason::new(
                "security items",
                "look at them",
            )],
            7,
            "",
        )
        .unwrap();
        item.insert(common::draft_hold::FIELD.into(), person.to_attr());
        assert_eq!(hold_of_run("r", &item, now()), None);
    }

    #[test]
    fn only_open_idle_runs_are_ended() {
        assert!(run_closes("needs_input"));
        assert!(run_closes("completed"));
        assert!(!run_closes("running"));
        assert!(!run_closes("awaiting_ci"));
        assert!(!run_closes("merged"));
        assert!(!run_closes("closed"));
    }
}
