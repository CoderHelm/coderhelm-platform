//! Chat's "what needs me": everything waiting on the person asking, from
//! every place Coderhelm reads, in one call ([`super::chat_work::NEEDS_ME`]).
//!
//! - Coderhelm's runs: questions a run stopped on (with the options it
//!   found), runs waiting on a change outside their repository, and
//!   Coderhelm's open pull requests on the person's tickets or runs.
//! - Approvals: tool calls held for an admin's yes (admins only, as the
//!   page that decides them).
//! - Jira, through the team's Jira app: open tickets assigned to them, and
//!   tickets where someone mentioned them after their last comment.
//! - GitHub, through the installation, in the team's repositories only:
//!   pull requests asking for their review, their own pull requests with
//!   changes asked for, failing checks or ready to merge, and comments that
//!   mention them they haven't answered.
//!
//! The person is who signed in: their email, their GitHub login, and the
//! Jira account Coderhelm has seen with their email. Every read is of the
//! asker's team only. The sources are read side by side, each within
//! [`SOURCE_BUDGET`]; one that fails, times out or isn't connected is named
//! in the result, never skipped quietly. Items are grouped by urgency and
//! ranked by priority and age; stale tickets are left out and counted.

use std::time::Duration;

use common::mcp_remote::http::{Http, HttpRequest, Limit};
use serde_json::{json, Value};

use super::chat_work::clip;
use super::chat_work_prs::{scoped_queries, Client};
use super::chat_work_runs::Item;
use super::chat_work_tickets::JiraApp;

/// How long one source may take before it's reported as not answering.
pub const SOURCE_BUDGET: Duration = Duration::from_secs(12);
/// Most items one answer lists.
pub const MAX_ITEMS: usize = 25;
/// An assigned ticket nobody touched for this long is noise, unless it's
/// urgent or someone asked the person something on it.
pub const STALE_DAYS: i64 = 60;
/// A run waiting this long is left out as noise (it's counted).
pub const OLD_RUN_DAYS: i64 = 30;
/// Own pull requests read in full (reviews, checks).
const MAX_OWN_PRS: usize = 4;
/// Pull requests whose comments are read for unanswered mentions.
const MAX_MENTION_PRS: usize = 3;

/// How soon something needs the person.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Urgency {
    /// Something is stopped on them.
    Now,
    Today,
    Later,
}

impl Urgency {
    pub fn heading(self) -> &'static str {
        match self {
            Urgency::Now => "Needs you now",
            Urgency::Today => "Today",
            Urgency::Later => "When you can",
        }
    }

    fn word(self) -> &'static str {
        match self {
            Urgency::Now => "now",
            Urgency::Today => "today",
            Urgency::Later => "when you can",
        }
    }
}

/// One thing waiting on the person.
#[derive(Debug, Clone, PartialEq)]
pub struct Need {
    pub urgency: Urgency,
    /// Higher first within its group (priority, then how long it waited).
    pub rank: i64,
    /// `run`, `ticket`, `pull_request` or `record` (an approval).
    pub kind: &'static str,
    /// What it is: "ABC-12 Fix the login redirect", "acme/web#41 Add slips".
    pub what: String,
    /// Exactly what the person does.
    pub action: String,
    /// Where they do it: the page outside Coderhelm, or the dashboard page.
    pub link: String,
    /// Since when it waits.
    pub since: String,
    /// More for the answer: a run's questions with their options.
    pub detail: String,
    /// The repository or place, for the citation.
    pub loc: String,
}

/// How a source was read.
#[derive(Debug, Clone, PartialEq)]
pub enum Read {
    /// Read; this many items came from it.
    Ok(usize),
    /// Tried and failed, or didn't answer in time: why.
    Failed(String),
    /// Not read: not connected, not linked or not allowed. Why, plainly.
    Off(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceNote {
    pub name: &'static str,
    pub read: Read,
}

/// Who is asking.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Me {
    pub email: String,
    pub github: Option<String>,
    /// Their Jira account, once known ([`Store::jira_accounts`]).
    pub jira_id: Option<String>,
    /// An admin or owner: decides held tool calls.
    pub is_admin: bool,
}

/// What Coderhelm keeps itself, read for one team only.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// The team's latest runs (newest first, bounded).
    async fn runs(&self, team: &str) -> Result<Vec<Item>, String>;
    /// The team's tool calls held for a person's yes, pending only.
    async fn approvals(&self, team: &str) -> Result<Vec<Value>, String>;
    /// The Jira account ids Coderhelm has seen with this email.
    async fn jira_accounts(&self, team: &str, email: &str) -> Vec<String>;
}

/// GitHub through the team's installation.
#[derive(Debug, Clone)]
pub struct GitHub {
    pub auth: String,
    pub base: String,
    /// GitHub said the hour's limit is spent: why.
    pub held: Option<String>,
}

/// Where one "what needs me" reads, all of it the asker's team's.
pub struct Sources<'a> {
    pub team: &'a str,
    pub store: &'a dyn Store,
    pub http: &'a dyn Http,
    pub jira: Option<&'a JiraApp>,
    /// Why `jira` is `None`, in words (`chat_work_tickets::Jira::off_words`).
    pub jira_off: &'a str,
    pub github: Option<GitHub>,
    /// The team's repositories the person may read.
    pub repos: &'a [String],
    /// The record kinds the person may read (`chat_work::readable`).
    pub readable: &'a [&'static str],
    /// How long each source may take ([`SOURCE_BUDGET`]).
    pub budget: Duration,
}

/// Everything found, grouped and ranked.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Report {
    pub needs: Vec<Need>,
    pub sources: Vec<SourceNote>,
    /// What was left out as noise, in words ("3 tickets untouched for 60+ days").
    pub skipped: Vec<String>,
    /// Who the items are for, in words.
    pub who: String,
}

// ── Time ────────────────────────────────────────────────────────────────────

type Time = chrono::DateTime<chrono::Utc>;

/// RFC 3339, or Jira's `2026-11-12T10:00:00.000+0000`, or a bare date.
pub fn parse_time(s: &str) -> Option<Time> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(t.with_timezone(&chrono::Utc));
    }
    if let Ok(t) = chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%z") {
        return Some(t.with_timezone(&chrono::Utc));
    }
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|d| d.and_utc())
}

fn days_since(s: &str, now: Time) -> i64 {
    parse_time(s).map_or(0, |t| (now - t).num_days().max(0))
}

/// "since Nov 14" for a line.
fn since_words(s: &str) -> String {
    parse_time(s).map_or(String::new(), |t| t.format("%b %-d").to_string())
}

// ── Coderhelm's runs ────────────────────────────────────────────────────────

fn st<'a>(item: &'a Item, k: &str) -> &'a str {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .unwrap_or("")
}

/// The run was started by this person, or for one of their tickets.
pub fn run_is_mine(item: &Item, me: &Me, my_tickets: &[String]) -> bool {
    let email = st(item, "requester_email");
    let key = st(item, "requester_key");
    let ticket = st(item, "ticket_id");
    (!me.email.is_empty() && email.eq_ignore_ascii_case(&me.email))
        || me
            .github
            .as_deref()
            .is_some_and(|g| key.eq_ignore_ascii_case(&format!("github:{g}")))
        || me
            .jira_id
            .as_deref()
            .is_some_and(|j| key.eq_ignore_ascii_case(&format!("jira:{j}")))
        || (!ticket.is_empty() && my_tickets.iter().any(|t| t.eq_ignore_ascii_case(ticket)))
}

fn run_what(item: &Item) -> String {
    let ticket = st(item, "ticket_id");
    let title = clip(st(item, "title"), 90);
    if ticket.is_empty() {
        title
    } else {
        format!("{ticket} {title}")
    }
}

/// The Jira account ids runs recorded with this email (who assigned them).
pub fn jira_ids_from_runs(items: &[Item], email: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for i in items {
        if email.is_empty() || !st(i, "requester_email").eq_ignore_ascii_case(email) {
            continue;
        }
        if let Some(id) = st(i, "requester_key").strip_prefix("jira:") {
            if !id.is_empty() && !out.iter().any(|o| o == id) {
                out.push(id.to_string());
            }
        }
    }
    out
}

/// What the team's runs need: answers, outside changes, and Coderhelm's
/// open pull requests on the person's own work. Returns the needs and how
/// many old waiting runs were left out.
pub fn from_runs(items: &[Item], me: &Me, my_tickets: &[String], now: Time) -> (Vec<Need>, usize) {
    use common::run_status as rs;
    let mut out = Vec::new();
    let mut old = 0;
    for i in items {
        let status = st(i, "status");
        let id = st(i, "run_id");
        if id.is_empty() {
            continue;
        }
        let page = format!("/runs/detail?id={}", urlencoding::encode(id));
        let updated = [st(i, "updated_at"), st(i, "created_at")]
            .into_iter()
            .find(|t| !t.is_empty())
            .unwrap_or("")
            .to_string();
        let age = days_since(&updated, now);
        let mine = run_is_mine(i, me, my_tickets);
        let by = st(i, "requester_name");
        let for_whom = if mine || by.is_empty() {
            String::new()
        } else {
            format!(" (started by {by})")
        };
        match status {
            rs::NEEDS_INPUT => {
                if age > OLD_RUN_DAYS {
                    old += 1;
                    continue;
                }
                let asked: Option<common::run_questions::Asked> = i
                    .get(common::run_questions::FIELD_QUESTIONS)
                    .and_then(|v| v.as_s().ok())
                    .and_then(|j| serde_json::from_str(j).ok());
                let (n, detail, since) = match &asked {
                    Some(a) if !a.questions.is_empty() => (
                        a.questions.len(),
                        common::run_questions::plain(&a.questions),
                        if a.asked_at.is_empty() {
                            updated.clone()
                        } else {
                            a.asked_at.clone()
                        },
                    ),
                    _ => (0, String::new(), updated.clone()),
                };
                let action = match n {
                    0 => "Answer what the run asked, on its page".to_string(),
                    1 => "Answer the run's question".to_string(),
                    n => format!("Answer the run's {n} questions"),
                };
                out.push(Need {
                    // A run started by someone else waits on them first.
                    urgency: if mine || by.is_empty() {
                        Urgency::Now
                    } else {
                        Urgency::Today
                    },
                    rank: 50 + days_since(&since, now).min(30),
                    kind: "run",
                    what: format!("{}{for_whom}", run_what(i)),
                    action,
                    link: page,
                    since,
                    detail,
                    loc: st(i, "repo").to_string(),
                });
            }
            rs::NEEDS_OUTSIDE_CHANGE => {
                if age > OLD_RUN_DAYS {
                    old += 1;
                    continue;
                }
                let why = [st(i, "error_message"), st(i, "error")]
                    .into_iter()
                    .find(|e| !e.trim().is_empty())
                    .unwrap_or("");
                out.push(Need {
                    urgency: if mine { Urgency::Today } else { Urgency::Later },
                    rank: 40 + age.min(30),
                    kind: "run",
                    what: format!("{}{for_whom}", run_what(i)),
                    action:
                        "Make the change outside the repository the run waits on, then resume it"
                            .into(),
                    link: page,
                    since: updated.clone(),
                    detail: clip(why, 400),
                    loc: st(i, "repo").to_string(),
                });
            }
            s if mine && [rs::COMPLETED, rs::SUCCESS].contains(&s) && age <= OLD_RUN_DAYS => {
                for p in common::run_prs::from_item(i)
                    .into_iter()
                    .filter(|p| p.outcome.is_empty() && !p.url.is_empty())
                {
                    out.push(Need {
                        urgency: Urgency::Today,
                        rank: 30 + age.min(30),
                        kind: "pull_request",
                        what: format!("{}#{} for {}", p.repo, p.pr_number, run_what(i)),
                        action: "Review and merge Coderhelm's pull request".into(),
                        link: p.url.clone(),
                        since: updated.clone(),
                        detail: String::new(),
                        loc: p.repo.clone(),
                    });
                }
            }
            _ => {}
        }
    }
    (out, old)
}

/// Tool calls held for an admin's yes.
pub fn from_approvals(asks: &[Value], now: Time) -> Vec<Need> {
    asks.iter()
        .filter(|a| a["status"] == "pending")
        .map(|a| {
            let tool = a["tool"].as_str().unwrap_or("a tool");
            let repo = a["repo"].as_str().unwrap_or("");
            let why = a["why"].as_str().unwrap_or("");
            let since = a["created_at"].as_str().unwrap_or("").to_string();
            Need {
                urgency: Urgency::Now,
                rank: 60 + days_since(&since, now).min(30),
                kind: "record",
                what: if repo.is_empty() {
                    format!("Tool call {tool}")
                } else {
                    format!("Tool call {tool} in {repo}")
                },
                action: "Approve or deny it before it expires".into(),
                link: "/data-sources/tools".into(),
                detail: clip(why, 300),
                since,
                loc: repo.to_string(),
            }
        })
        .collect()
}

// ── Jira ────────────────────────────────────────────────────────────────────

/// Asks the team's Jira app what is waiting on one account.
pub async fn ask_jira_mine(http: &dyn Http, app: &JiraApp, account: &str) -> Result<Value, String> {
    let body = json!({ "forge_secret": app.secret, "mine": { "account_id": account } });
    let r = http
        .send(HttpRequest::json(&app.issues_url, &body, Limit::Metadata))
        .await
        .map_err(|e| format!("Jira couldn't be reached: {e}"))?;
    if r.status != 200 {
        return Err(format!("Jira's app answered {}", r.status));
    }
    let v = r.json().ok_or("Jira's app sent something unreadable")?;
    if !v["mine"].is_object() {
        return Err(
            "the team's Jira app is an older version that can't list a person's tickets; an admin updates it in Atlassian Administration"
                .into(),
        );
    }
    Ok(v)
}

/// How much a Jira priority weighs (higher is more urgent).
fn priority_weight(p: &str) -> i64 {
    match p.trim().to_ascii_lowercase().as_str() {
        "highest" | "blocker" | "critical" | "urgent" => 4,
        "high" | "major" => 3,
        "low" | "minor" | "lowest" | "trivial" => 1,
        _ => 2,
    }
}

/// The newest comment that mentions `account`, by someone else, with no
/// later comment of theirs: `(author, text, created)`.
pub fn unanswered_mention(issue: &Value, account: &str) -> Option<(String, String, String)> {
    let comments = issue["comments"].as_array()?;
    let mut asked: Option<(String, String, String)> = None;
    for c in comments {
        let author = c["author_id"].as_str().unwrap_or("");
        if author == account {
            asked = None;
            continue;
        }
        let mentions = c["mentions"]
            .as_array()
            .is_some_and(|m| m.iter().any(|x| x == account));
        if mentions {
            asked = Some((
                c["author"].as_str().unwrap_or("someone").to_string(),
                c["text"].as_str().unwrap_or("").to_string(),
                c["created"].as_str().unwrap_or("").to_string(),
            ));
        }
    }
    asked
}

/// What the Jira app's answer holds for this account: the needs, the keys
/// assigned to them, and how many stale tickets were left out.
pub fn from_jira(
    resp: &Value,
    account: &str,
    site: &str,
    now: Time,
) -> (Vec<Need>, Vec<String>, usize) {
    let mut out = Vec::new();
    let mut assigned: Vec<String> = Vec::new();
    let mut stale = 0;
    for i in resp["issues"].as_array().into_iter().flatten() {
        let s = |k: &str| i[k].as_str().unwrap_or("").trim().to_string();
        let key = s("key");
        if !super::chat_work_tickets::valid_key(&key) || s("status_category") == "done" {
            continue;
        }
        let link = if site.is_empty() {
            String::new()
        } else {
            format!("{}/browse/{key}", site.trim_end_matches('/'))
        };
        let what = format!("{key} {}", clip(&s("summary"), 90));
        let weight = priority_weight(&s("priority"));
        let asked = unanswered_mention(i, account);
        let mine = s("assignee_id") == account;
        if !mine {
            // Only a question to them makes someone else's ticket theirs.
            if let Some((who, text, at)) = asked {
                out.push(Need {
                    urgency: Urgency::Today,
                    rank: 20 + weight * 10 + days_since(&at, now).min(30),
                    kind: "ticket",
                    what,
                    action: format!(
                        "Reply to {who}, who mentioned you: \u{201c}{}\u{201d}",
                        clip(&text, 160)
                    ),
                    link,
                    since: at,
                    detail: String::new(),
                    loc: "Jira".into(),
                });
            }
            continue;
        }
        assigned.push(key.clone());
        let updated = s("updated");
        let due = s("due");
        let overdue = parse_time(&due).is_some_and(|d| d < now);
        let due_soon = parse_time(&due).is_some_and(|d| d >= now && (d - now).num_days() <= 2);
        if asked.is_none() && weight <= 2 && !overdue && days_since(&updated, now) > STALE_DAYS {
            stale += 1;
            continue;
        }
        let urgency = if weight >= 4 || overdue {
            Urgency::Now
        } else if weight == 3 || asked.is_some() || due_soon {
            Urgency::Today
        } else {
            Urgency::Later
        };
        let status = s("status");
        let mut action = match &asked {
            Some((who, text, _)) => {
                format!(
                    "Reply to {who}, who asked you: \u{201c}{}\u{201d}",
                    clip(text, 160)
                )
            }
            None if s("status_category") == "new" => {
                format!("Start it or say when you will (it's {status}, assigned to you)")
            }
            None => format!("Move it forward (it's {status}, assigned to you)"),
        };
        if overdue {
            action.push_str(&format!("; it was due {due}"));
        } else if due_soon {
            action.push_str(&format!("; due {due}"));
        }
        let priority = s("priority");
        out.push(Need {
            urgency,
            rank: weight * 10 + days_since(&updated, now).min(30),
            kind: "ticket",
            what: if priority.is_empty() {
                what
            } else {
                format!("{what} ({priority} priority)")
            },
            action,
            link,
            since: asked.map(|a| a.2).unwrap_or(updated),
            detail: String::new(),
            loc: "Jira".into(),
        });
    }
    (out, assigned, stale)
}

// ── GitHub ──────────────────────────────────────────────────────────────────

fn repo_of_hit(hit: &Value) -> String {
    hit["repository_url"]
        .as_str()
        .and_then(|u| u.split("/repos/").nth(1))
        .unwrap_or("")
        .to_string()
}

/// The search hits in the team's repositories (whatever the search sent).
async fn search(
    client: &mut Client<'_>,
    head: &str,
    repos: &[String],
) -> Result<Vec<(String, Value)>, String> {
    let mut out: Vec<(String, Value)> = Vec::new();
    for q in scoped_queries(head, repos) {
        let path = format!(
            "/search/issues?q={}&sort=updated&order=desc&per_page=20",
            urlencoding::encode(&q)
        );
        if let Some(v) = client.get(&path).await? {
            for h in v["items"].as_array().into_iter().flatten() {
                let repo = repo_of_hit(h);
                if let Some(r) = repos.iter().find(|r| r.eq_ignore_ascii_case(&repo)) {
                    out.push((r.clone(), h.clone()));
                }
            }
        }
    }
    Ok(out)
}

fn pr_what(repo: &str, pr: &Value) -> String {
    format!(
        "{repo}#{} {}",
        pr["number"].as_u64().unwrap_or(0),
        clip(pr["title"].as_str().unwrap_or(""), 90)
    )
}

/// What one of the person's own open pull requests needs from them, from its
/// reviews and checks. `None` when it waits on others.
pub fn own_pr_need(
    repo: &str,
    pr: &Value,
    reviews: &Value,
    checks: &Value,
    now: Time,
) -> Option<Need> {
    let mut latest: Vec<(String, String)> = Vec::new();
    for r in reviews.as_array().into_iter().flatten() {
        let who = r["user"]["login"].as_str().unwrap_or("?").to_string();
        let state = r["state"].as_str().unwrap_or("").to_string();
        if state == "COMMENTED" || state == "PENDING" {
            continue;
        }
        latest.retain(|(w, _)| *w != who);
        latest.push((who, state));
    }
    let asked: Vec<&str> = latest
        .iter()
        .filter(|(_, s)| s == "CHANGES_REQUESTED")
        .map(|(w, _)| w.as_str())
        .collect();
    let approved = latest.iter().any(|(_, s)| s == "APPROVED");
    let runs = checks["check_runs"].as_array().cloned().unwrap_or_default();
    let failed: Vec<&str> = runs
        .iter()
        .filter(|r| {
            r["status"] == "completed"
                && !matches!(
                    r["conclusion"].as_str(),
                    Some("success" | "neutral" | "skipped")
                )
        })
        .filter_map(|r| r["name"].as_str())
        .take(4)
        .collect();
    let running = runs.iter().any(|r| r["status"] != "completed");
    let updated = pr["updated_at"].as_str().unwrap_or("").to_string();
    let age = days_since(&updated, now).min(30);
    let (urgency, rank, action) = if !asked.is_empty() {
        (
            Urgency::Today,
            35 + age,
            format!("Make the changes {} asked for", asked.join(" and ")),
        )
    } else if !failed.is_empty() {
        (
            Urgency::Today,
            30 + age,
            format!("Fix the failing checks: {}", failed.join(", ")),
        )
    } else if approved && !running && pr["draft"] != true {
        (
            Urgency::Later,
            20 + age,
            "Merge it: it's approved and its checks pass".to_string(),
        )
    } else {
        return None;
    };
    Some(Need {
        urgency,
        rank,
        kind: "pull_request",
        what: pr_what(repo, pr),
        action,
        link: pr["html_url"].as_str().unwrap_or("").to_string(),
        since: updated,
        detail: String::new(),
        loc: repo.to_string(),
    })
}

/// The newest comment mentioning `@login` by someone else, with no later
/// comment by them, among a pull request's comments (any order).
pub fn unanswered_github(comments: &[Value], login: &str) -> Option<Value> {
    let mut all: Vec<&Value> = comments.iter().collect();
    all.sort_by(|a, b| a["created_at"].as_str().cmp(&b["created_at"].as_str()));
    let tag = format!("@{}", login.to_lowercase());
    let mut asked: Option<&Value> = None;
    for c in all {
        let author = c["user"]["login"].as_str().unwrap_or("");
        if author.eq_ignore_ascii_case(login) {
            asked = None;
        } else if c["user"]["type"] != "Bot" {
            let body = c["body"].as_str().unwrap_or("").to_lowercase();
            let named = body.match_indices(&tag).any(|(i, _)| {
                !body[i + tag.len()..]
                    .chars()
                    .next()
                    .is_some_and(|ch| ch.is_alphanumeric() || ch == '-')
            });
            if named {
                asked = Some(c);
            }
        }
    }
    asked.cloned()
}

fn client<'a>(http: &'a dyn Http, gh: &GitHub) -> Client<'a> {
    Client {
        http,
        auth: gh.auth.clone(),
        base: gh.base.clone(),
        sent: 0,
        held: gh.held.clone(),
    }
}

/// Reviews asked of them.
async fn review_requests(
    http: &dyn Http,
    gh: &GitHub,
    login: &str,
    repos: &[String],
    now: Time,
) -> Result<Vec<Need>, String> {
    let mut c = client(http, gh);
    let hits = search(
        &mut c,
        &format!("is:pr is:open archived:false review-requested:{login}"),
        repos,
    )
    .await?;
    Ok(hits
        .into_iter()
        .filter(|(_, h)| h["draft"] != true)
        .map(|(repo, h)| {
            let since = h["updated_at"].as_str().unwrap_or("").to_string();
            let age = days_since(&since, now);
            let by = h["user"]["login"].as_str().unwrap_or("");
            Need {
                // Someone waited two days on them: it blocks them now.
                urgency: if age >= 2 {
                    Urgency::Now
                } else {
                    Urgency::Today
                },
                rank: 40 + age.min(30),
                kind: "pull_request",
                what: if by.is_empty() {
                    pr_what(&repo, &h)
                } else {
                    format!("{} by {by}", pr_what(&repo, &h))
                },
                action: "Review it: your review was asked for".into(),
                link: h["html_url"].as_str().unwrap_or("").to_string(),
                since,
                detail: String::new(),
                loc: repo,
            }
        })
        .collect())
}

/// Their own open pull requests that need them.
async fn own_prs(
    http: &dyn Http,
    gh: &GitHub,
    login: &str,
    repos: &[String],
    now: Time,
) -> Result<Vec<Need>, String> {
    let mut c = client(http, gh);
    let hits = search(
        &mut c,
        &format!("is:pr is:open archived:false author:{login}"),
        repos,
    )
    .await?;
    let mut out = Vec::new();
    for (repo, h) in hits.into_iter().take(MAX_OWN_PRS) {
        let n = h["number"].as_u64().unwrap_or(0);
        if n == 0 {
            continue;
        }
        let mut c = client(http, gh);
        let pr = c
            .get(&format!("/repos/{repo}/pulls/{n}"))
            .await?
            .unwrap_or(h);
        let reviews = c
            .get(&format!("/repos/{repo}/pulls/{n}/reviews?per_page=50"))
            .await?
            .unwrap_or(Value::Null);
        let checks = match pr["head"]["sha"].as_str() {
            Some(sha) => c
                .get(&format!(
                    "/repos/{repo}/commits/{sha}/check-runs?per_page=50"
                ))
                .await?
                .unwrap_or(Value::Null),
            None => Value::Null,
        };
        out.extend(own_pr_need(&repo, &pr, &reviews, &checks, now));
    }
    Ok(out)
}

/// Comments mentioning them on open pull requests they haven't answered.
async fn mentions(
    http: &dyn Http,
    gh: &GitHub,
    login: &str,
    repos: &[String],
    now: Time,
) -> Result<Vec<Need>, String> {
    let mut c = client(http, gh);
    let hits = search(
        &mut c,
        &format!("is:pr is:open archived:false mentions:{login}"),
        repos,
    )
    .await?;
    let mut out = Vec::new();
    for (repo, h) in hits.into_iter().take(MAX_MENTION_PRS) {
        let n = h["number"].as_u64().unwrap_or(0);
        if n == 0 {
            continue;
        }
        let mut c = client(http, gh);
        let mut comments: Vec<Value> = Vec::new();
        for path in [
            format!("/repos/{repo}/issues/{n}/comments?per_page=50"),
            format!("/repos/{repo}/pulls/{n}/comments?per_page=50"),
        ] {
            if let Some(v) = c.get(&path).await? {
                comments.extend(v.as_array().cloned().unwrap_or_default());
            }
        }
        if let Some(cm) = unanswered_github(&comments, login) {
            let who = cm["user"]["login"].as_str().unwrap_or("someone");
            let at = cm["created_at"].as_str().unwrap_or("").to_string();
            out.push(Need {
                urgency: Urgency::Today,
                rank: 30 + days_since(&at, now).min(30),
                kind: "pull_request",
                what: pr_what(&repo, &h),
                action: format!(
                    "Reply to {who}'s comment: \u{201c}{}\u{201d}",
                    clip(&cm["body"].as_str().unwrap_or("").replace('\n', " "), 160)
                ),
                link: cm["html_url"]
                    .as_str()
                    .or(h["html_url"].as_str())
                    .unwrap_or("")
                    .to_string(),
                since: at,
                detail: String::new(),
                loc: repo,
            });
        }
    }
    Ok(out)
}

/// Everything GitHub holds for them. A part that fails is named; all three
/// failing is a failure.
async fn github_needs(
    http: &dyn Http,
    gh: &GitHub,
    login: &str,
    repos: &[String],
    now: Time,
) -> (Vec<Need>, Read) {
    let (a, b, c) = tokio::join!(
        review_requests(http, gh, login, repos, now),
        own_prs(http, gh, login, repos, now),
        mentions(http, gh, login, repos, now),
    );
    let mut out = Vec::new();
    let mut failed: Vec<String> = Vec::new();
    for (what, r) in [
        ("review requests", a),
        ("your pull requests", b),
        ("mentions", c),
    ] {
        match r {
            Ok(n) => out.extend(n),
            Err(e) => failed.push(format!("{what} ({e})")),
        }
    }
    let read = match failed.len() {
        0 => Read::Ok(out.len()),
        3 => Read::Failed(failed.remove(0)),
        _ => Read::Failed(format!("only partly: couldn't read {}", failed.join(", "))),
    };
    (out, read)
}

// ── Putting it together ─────────────────────────────────────────────────────

/// Runs `f` within `budget`; a timeout is a failure that says so.
async fn timed<T>(
    budget: Duration,
    what: &str,
    f: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    tokio::time::timeout(budget, f).await.map_err(|_| {
        format!(
            "{what} didn't answer within {} seconds",
            budget.as_secs().max(1)
        )
    })
}

fn may(readable: &[&str], k: &str) -> bool {
    readable.contains(&k)
}

/// Reads every source for this person, side by side, and ranks what needs
/// them. Only `src.team`'s records are read.
pub async fn gather(src: &Sources<'_>, mut me: Me, now: Time) -> Report {
    let mut sources: Vec<SourceNote> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    let can_runs = may(src.readable, "runs");
    // First what Coderhelm keeps: the runs, the held calls, the person's
    // Jira account.
    let (runs, asks, known) = tokio::join!(
        async {
            if can_runs {
                Some(timed(src.budget, "Coderhelm's runs", src.store.runs(src.team)).await)
            } else {
                None
            }
        },
        async {
            if me.is_admin {
                Some(timed(src.budget, "Approvals", src.store.approvals(src.team)).await)
            } else {
                None
            }
        },
        async {
            if me.email.is_empty() {
                Vec::new()
            } else {
                timed(
                    src.budget,
                    "Jira users",
                    src.store.jira_accounts(src.team, &me.email),
                )
                .await
                .unwrap_or_default()
            }
        },
    );
    let runs: Result<Vec<Item>, String> = match runs {
        Some(Ok(r)) => r,
        Some(Err(e)) => Err(e),
        None => Ok(Vec::new()),
    };
    if me.jira_id.is_none() {
        me.jira_id = known.into_iter().next().or_else(|| {
            runs.as_ref()
                .ok()
                .and_then(|r| jira_ids_from_runs(r, &me.email).into_iter().next())
        });
    }

    // Then the outside systems, side by side.
    let jira_fut = async {
        if !may(src.readable, "tickets") {
            return (
                Vec::new(),
                Vec::new(),
                0,
                Read::Off("your role can't see tickets".into()),
            );
        }
        let Some(app) = src.jira else {
            return (
                Vec::new(),
                Vec::new(),
                0,
                Read::Off(if src.jira_off.is_empty() {
                    common::jira_connection::NOT_CONNECTED.into()
                } else {
                    src.jira_off.to_string()
                }),
            );
        };
        let Some(account) = me.jira_id.clone() else {
            return (
                Vec::new(),
                Vec::new(),
                0,
                Read::Off(format!(
                    "Coderhelm hasn't seen a Jira account with your email ({}) yet",
                    me.email
                )),
            );
        };
        match timed(src.budget, "Jira", ask_jira_mine(src.http, app, &account)).await {
            Ok(Ok(resp)) => {
                let (n, keys, stale) = from_jira(&resp, &account, &app.site, now);
                let read = Read::Ok(n.len());
                (n, keys, stale, read)
            }
            Ok(Err(e)) | Err(e) => (Vec::new(), Vec::new(), 0, Read::Failed(e)),
        }
    };
    let github_fut = async {
        if !may(src.readable, "pull_requests") {
            return (
                Vec::new(),
                Read::Off("your role can't see pull requests".into()),
            );
        }
        let Some(gh) = &src.github else {
            return (
                Vec::new(),
                Read::Off("Coderhelm can't reach GitHub for this team right now".into()),
            );
        };
        let Some(login) = me.github.clone().filter(|l| {
            !l.is_empty()
                && l.len() <= 39
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        }) else {
            return (
                Vec::new(),
                Read::Off("your Coderhelm sign-in isn't linked to a GitHub account".into()),
            );
        };
        if src.repos.is_empty() {
            return (
                Vec::new(),
                Read::Off("no repositories you can read are connected".into()),
            );
        }
        match timed(
            src.budget,
            "GitHub",
            github_needs(src.http, gh, &login, src.repos, now),
        )
        .await
        {
            Ok(r) => r,
            Err(e) => (Vec::new(), Read::Failed(e)),
        }
    };
    let ((jira_needs, my_tickets, stale, jira_read), (gh_needs, gh_read)) =
        tokio::join!(jira_fut, github_fut);

    let mut needs: Vec<Need> = Vec::new();
    match (&runs, can_runs) {
        (_, false) => sources.push(SourceNote {
            name: "Coderhelm's runs",
            read: Read::Off("your role can't see runs".into()),
        }),
        (Ok(items), true) => {
            let (n, old) = from_runs(items, &me, &my_tickets, now);
            if old > 0 {
                skipped.push(format!(
                    "{old} run(s) waiting more than {OLD_RUN_DAYS} days"
                ));
            }
            sources.push(SourceNote {
                name: "Coderhelm's runs",
                read: Read::Ok(n.len()),
            });
            needs.extend(n);
        }
        (Err(e), true) => sources.push(SourceNote {
            name: "Coderhelm's runs",
            read: Read::Failed(e.clone()),
        }),
    }
    match asks {
        Some(Ok(Ok(a))) => {
            let n = from_approvals(&a, now);
            sources.push(SourceNote {
                name: "Approvals",
                read: Read::Ok(n.len()),
            });
            needs.extend(n);
        }
        Some(Ok(Err(e)) | Err(e)) => sources.push(SourceNote {
            name: "Approvals",
            read: Read::Failed(e),
        }),
        None => {}
    }
    if stale > 0 {
        skipped.push(format!(
            "{stale} assigned ticket(s) nobody touched in {STALE_DAYS}+ days"
        ));
    }
    sources.push(SourceNote {
        name: "Jira",
        read: jira_read,
    });
    needs.extend(jira_needs);
    sources.push(SourceNote {
        name: "GitHub",
        read: gh_read,
    });
    needs.extend(gh_needs);

    let who = {
        let mut parts = vec![me.email.clone()];
        if let Some(g) = &me.github {
            parts.push(format!("GitHub @{g}"));
        }
        if me.jira_id.is_some() {
            parts.push("your Jira account".into());
        }
        parts.retain(|p| !p.is_empty());
        parts.join(", ")
    };
    Report {
        needs: rank(needs),
        sources,
        skipped,
        who,
    }
}

/// One entry per link (the most urgent kept), most urgent first, then by
/// rank, capped at [`MAX_ITEMS`].
pub fn rank(mut needs: Vec<Need>) -> Vec<Need> {
    needs.sort_by(|a, b| a.urgency.cmp(&b.urgency).then(b.rank.cmp(&a.rank)));
    let mut seen: Vec<String> = Vec::new();
    needs.retain(|n| {
        if n.link.is_empty() {
            return true;
        }
        let k = n.link.to_lowercase();
        let fresh = !seen.contains(&k);
        seen.push(k);
        fresh
    });
    needs.truncate(MAX_ITEMS);
    needs
}

/// The count line: "6 things need you: 2 now, 3 today, 1 when you can."
pub fn count_line(needs: &[Need]) -> String {
    if needs.is_empty() {
        return "Nothing is waiting on you right now in the places Coderhelm could read.".into();
    }
    let parts: Vec<String> = [Urgency::Now, Urgency::Today, Urgency::Later]
        .into_iter()
        .filter_map(|u| {
            let n = needs.iter().filter(|x| x.urgency == u).count();
            (n > 0).then(|| format!("{n} {}", u.word()))
        })
        .collect();
    format!(
        "{} {} you: {}.",
        needs.len(),
        if needs.len() == 1 {
            "thing needs"
        } else {
            "things need"
        },
        parts.join(", ")
    )
}

/// A source's state in words, for the answer.
fn read_words(r: &Read) -> String {
    match r {
        Read::Ok(_) => "read".into(),
        Read::Failed(e) => format!("couldn't be read ({e})"),
        Read::Off(why) => format!("not checked: {why}"),
    }
}

/// How the answer is written.
pub const ANSWER_SHAPE: &str = "Write the answer in exactly this shape. First the count line. Then each group in the order given (skip an empty one), with its heading, and one line per item, most urgent first: a markdown link to the item named by what it is, then exactly what they need to do, then since when, and its citation. Under a run's line, keep its questions with their possible answers as a short nested list. Then, for every source that couldn't be read or wasn't checked, one plain sentence that says so (for example \"GitHub couldn't be read, so your pull requests aren't listed\"), in that source's own words: never say a system isn't connected unless its line says exactly that. Don't add items that aren't listed here.";

/// Registers each item as a citable source and writes what the model reads.
/// Returns the text and the step's summary.
pub fn render(
    report: &Report,
    reg: &mut common::knowledge::answer::Registry,
    stamp: &str,
) -> (String, String) {
    use common::knowledge::answer::SourceRef;
    let mut text = format!(
        "What needs you, as of {stamp} (for {}).\nCount line: {}\n",
        report.who,
        count_line(&report.needs)
    );
    let mut ids: Vec<String> = Vec::new();
    for u in [Urgency::Now, Urgency::Today, Urgency::Later] {
        let group: Vec<&Need> = report.needs.iter().filter(|n| n.urgency == u).collect();
        if group.is_empty() {
            continue;
        }
        text.push_str(&format!("\n## {}\n", u.heading()));
        for n in group {
            let since = since_words(&n.since);
            let mut passage = format!("{}\nTo do: {}\n", n.what, n.action);
            if !since.is_empty() {
                passage.push_str(&format!("Waiting since: {since}\n"));
            }
            if !n.detail.is_empty() {
                passage.push_str(&format!("{}\n", n.detail));
            }
            if !n.link.is_empty() {
                passage.push_str(&format!("Link: {}\n", n.link));
            }
            let (href, url) = if n.link.starts_with('/') {
                (Some(n.link.clone()), None)
            } else {
                (None, common::knowledge::answer::original_url(&n.link))
            };
            let id = reg.add(SourceRef {
                kind: n.kind.into(),
                title: clip(&n.what, 120),
                loc: n.loc.clone(),
                as_of: n.since.clone(),
                passage,
                href,
                url,
                ..Default::default()
            });
            text.push_str(&format!(
                "- [{id}] [{}]({}) | {}{}\n",
                n.what,
                n.link,
                n.action,
                if since.is_empty() {
                    String::new()
                } else {
                    format!(" | since {since}")
                }
            ));
            if !n.detail.is_empty() && n.kind == "run" {
                for l in n.detail.lines().filter(|l| !l.trim().is_empty()) {
                    text.push_str(&format!("    {l}\n"));
                }
            }
            ids.push(id);
        }
    }
    text.push_str("\nSources:\n");
    for s in &report.sources {
        text.push_str(&format!("- {}: {}\n", s.name, read_words(&s.read)));
    }
    if !report.skipped.is_empty() {
        text.push_str(&format!(
            "Left out as noise: {}.\n",
            report.skipped.join("; ")
        ));
    }
    text.push_str(&format!("\n{ANSWER_SHAPE}\n\n"));
    text.push_str(&reg.render(&ids));
    let gaps = report
        .sources
        .iter()
        .filter(|s| !matches!(s.read, Read::Ok(_)))
        .count();
    let mut summary = match report.needs.len() {
        0 => "Nothing waiting".to_string(),
        1 => "1 item".to_string(),
        n => format!("{n} items"),
    };
    if gaps > 0 {
        summary.push_str(&format!(
            ", {gaps} {} not read",
            if gaps == 1 { "source" } else { "sources" }
        ));
    }
    (text, summary)
}

// ── Production store ────────────────────────────────────────────────────────

/// The store on DynamoDB: the runs table and the settings table, each read
/// by the team's partition key only.
pub struct Dynamo<'a> {
    pub state: &'a crate::AppState,
}

/// Runs read when looking for what waits (newest first).
const RUN_SCAN: usize = 300;

#[async_trait::async_trait]
impl Store for Dynamo<'_> {
    async fn runs(&self, team: &str) -> Result<Vec<Item>, String> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let attrs: Vec<&str> = super::run_search::LIST_ATTRS
            .iter()
            .copied()
            .chain([
                "updated_at",
                common::run_prs::FIELD,
                "pr_urls",
                "branches",
                "error_message",
                "error",
                common::run_questions::FIELD_QUESTIONS,
            ])
            .collect();
        let names: std::collections::HashMap<String, String> = attrs
            .iter()
            .enumerate()
            .map(|(i, a)| (format!("#a{i}"), a.to_string()))
            .collect();
        let projection = (0..attrs.len())
            .map(|i| format!("#a{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut items: Vec<Item> = Vec::new();
        let mut start: Option<Item> = None;
        loop {
            let r = self
                .state
                .dynamo
                .query()
                .table_name(&self.state.config.runs_table_name)
                .key_condition_expression("team_id = :tid")
                .expression_attribute_values(":tid", AttributeValue::S(team.to_string()))
                .projection_expression(&projection)
                .set_expression_attribute_names(Some(names.clone()))
                .scan_index_forward(false)
                .limit(100)
                .set_exclusive_start_key(start.take())
                .send()
                .await
                .map_err(|e| {
                    format!("runs: {}", aws_sdk_dynamodb::error::DisplayErrorContext(&e))
                })?;
            items.extend(r.items().iter().cloned());
            match r.last_evaluated_key() {
                Some(k) if !k.is_empty() && items.len() < RUN_SCAN => start = Some(k.clone()),
                _ => break,
            }
        }
        Ok(items)
    }

    async fn approvals(&self, team: &str) -> Result<Vec<Value>, String> {
        let st = common::live::tools_store::ToolStore {
            dynamo: &self.state.dynamo,
            table: &self.state.config.settings_table_name,
            team,
        };
        Ok(st
            .asks()
            .await?
            .into_iter()
            .filter(|a| a["status"] == "pending")
            .collect())
    }

    async fn jira_accounts(&self, team: &str, email: &str) -> Vec<String> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let prefix = common::requester::JIRA_USER_PREFIX;
        let mut out: Vec<String> = Vec::new();
        let mut start: Option<Item> = None;
        for _ in 0..5 {
            let r = self
                .state
                .dynamo
                .query()
                .table_name(&self.state.config.settings_table_name)
                .key_condition_expression("pk = :p AND begins_with(sk, :s)")
                .expression_attribute_values(":p", AttributeValue::S(team.to_string()))
                .expression_attribute_values(":s", AttributeValue::S(prefix.to_string()))
                .set_exclusive_start_key(start.take())
                .send()
                .await;
            let Ok(r) = r else {
                tracing::warn!("Chat: Jira users couldn't be read for what needs the asker");
                break;
            };
            for i in r.items() {
                if st(i, "email").eq_ignore_ascii_case(email) {
                    let id = st(i, "sk").trim_start_matches(prefix).to_string();
                    if !id.is_empty() && !out.contains(&id) {
                        out.push(id);
                    }
                }
            }
            match r.last_evaluated_key() {
                Some(k) if !k.is_empty() => start = Some(k.clone()),
                _ => break,
            }
        }
        out
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::routes::chat_work_prs::tests::fake;
    use crate::routes::chat_work_runs::tests::run;
    use aws_sdk_dynamodb::types::AttributeValue;
    use std::sync::Mutex;

    pub const TEAM: &str = "TEAM#a";
    pub const OTHER: &str = "TEAM#b";
    pub const ME: &str = "dana@example.com";
    pub const JIRA_ME: &str = "557058:aaaa1111-0000-0000-0000-000000000001";

    pub fn now() -> Time {
        parse_time("2026-11-16T12:00:00Z").unwrap()
    }

    /// Two teams' runs, held calls and Jira users.
    #[derive(Default)]
    pub struct FakeStore {
        pub runs: Vec<(String, Item)>,
        pub asks: Vec<(String, Value)>,
        pub jira_users: Vec<(String, String, String)>,
        pub fail_runs: bool,
        /// Teams each call was asked for.
        pub asked: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Store for FakeStore {
        async fn runs(&self, team: &str) -> Result<Vec<Item>, String> {
            self.asked.lock().unwrap().push(team.into());
            if self.fail_runs {
                return Err("runs: AccessDenied".into());
            }
            Ok(self
                .runs
                .iter()
                .filter(|(t, _)| t == team)
                .map(|(_, i)| i.clone())
                .collect())
        }
        async fn approvals(&self, team: &str) -> Result<Vec<Value>, String> {
            self.asked.lock().unwrap().push(team.into());
            Ok(self
                .asks
                .iter()
                .filter(|(t, _)| t == team)
                .map(|(_, a)| a.clone())
                .collect())
        }
        async fn jira_accounts(&self, team: &str, email: &str) -> Vec<String> {
            self.asked.lock().unwrap().push(team.into());
            self.jira_users
                .iter()
                .filter(|(t, _, e)| t == team && e.eq_ignore_ascii_case(email))
                .map(|(_, id, _)| id.clone())
                .collect()
        }
    }

    fn with(mut i: Item, extra: &[(&str, &str)]) -> Item {
        for (k, v) in extra {
            i.insert(k.to_string(), AttributeValue::S(v.to_string()));
        }
        i
    }

    pub fn waiting_run(id: &str, ticket: &str, requester: &str) -> Item {
        let q = json!({"questions": [{"id": "q1", "question": "Which retry limit should the job use?",
            "why": "The ticket doesn't say", "suggestions": ["3 tries", "5 tries"]}],
            "asked_at": "2026-11-14T09:00:00Z"});
        with(
            run(
                id,
                "needs_input",
                ticket,
                "Retry the export job",
                "acme/web",
            ),
            &[
                ("questions", &q.to_string()),
                ("updated_at", "2026-11-14T09:00:00Z"),
                ("requester_email", requester),
                ("requester_name", "Sam"),
            ],
        )
    }

    pub fn store() -> FakeStore {
        FakeStore {
            runs: vec![
                (TEAM.into(), waiting_run("01A", "ABC-1", ME)),
                (
                    TEAM.into(),
                    with(
                        run(
                            "01B",
                            "needs_outside_change",
                            "ABC-2",
                            "Rotate the signing key",
                            "acme/api",
                        ),
                        &[
                            ("updated_at", "2026-11-15T09:00:00Z"),
                            ("error_message", "An admin must add the key to the vault"),
                            ("requester_email", ME),
                        ],
                    ),
                ),
                (
                    TEAM.into(),
                    with(
                        run(
                            "01C",
                            "needs_input",
                            "ABC-3",
                            "Very old question",
                            "acme/web",
                        ),
                        &[("updated_at", "2026-08-01T09:00:00Z")],
                    ),
                ),
                (OTHER.into(), waiting_run("09Z", "OTH-9", ME)),
            ],
            asks: vec![
                (
                    TEAM.into(),
                    json!({"status": "pending", "tool": "restart_worker", "repo": "acme/api",
                        "why": "Restart the stuck worker", "created_at": "2026-11-16T11:00:00Z"}),
                ),
                (
                    OTHER.into(),
                    json!({"status": "pending", "tool": "drop_table", "repo": "other/secret",
                        "why": "secret", "created_at": "2026-11-16T11:00:00Z"}),
                ),
            ],
            jira_users: vec![
                (TEAM.into(), JIRA_ME.into(), ME.into()),
                (OTHER.into(), "557058:bbbb".into(), ME.into()),
            ],
            ..Default::default()
        }
    }

    pub fn app() -> JiraApp {
        JiraApp {
            issues_url: "https://jira-app.example.test/issues".into(),
            secret: "s3cret".into(),
            site: "https://team.atlassian.net".into(),
        }
    }

    fn comment(author: &str, author_id: &str, mentions: &[&str], text: &str, at: &str) -> Value {
        json!({"author": author, "author_id": author_id, "mentions": mentions, "text": text, "created": at})
    }

    pub fn jira_answer() -> Value {
        json!({
            "mine": {"found": true, "account_id": JIRA_ME},
            "issues": [
                {"key": "ABC-10", "summary": "Payments fail on retry", "status": "In Progress", "status_category": "indeterminate",
                 "priority": "Highest", "assignee_id": JIRA_ME, "updated": "2026-11-15T10:00:00.000+0000"},
                {"key": "ABC-11", "summary": "Tidy the footer", "status": "To Do", "status_category": "new",
                 "priority": "Low", "assignee_id": JIRA_ME, "updated": "2026-11-10T10:00:00.000+0000"},
                {"key": "ABC-12", "summary": "Untouched idea", "status": "To Do", "status_category": "new",
                 "priority": "Medium", "assignee_id": JIRA_ME, "updated": "2026-05-01T10:00:00.000+0000"},
                {"key": "ABC-13", "summary": "Pick the cache size", "status": "In Review", "status_category": "indeterminate",
                 "priority": "Medium", "assignee_id": "557058:someone-else", "updated": "2026-11-15T10:00:00.000+0000",
                 "comments": [comment("Sam", "557058:someone-else", &[JIRA_ME], "Can you confirm 64 MB?", "2026-11-15T09:00:00.000+0000")]},
                {"key": "ABC-14", "summary": "Answered already", "status": "In Review", "status_category": "indeterminate",
                 "assignee_id": "557058:someone-else", "updated": "2026-11-15T10:00:00.000+0000",
                 "comments": [comment("Sam", "557058:someone-else", &[JIRA_ME], "Thoughts?", "2026-11-14T09:00:00.000+0000"),
                              comment("Dana", JIRA_ME, &[], "Looks good", "2026-11-14T10:00:00.000+0000")]}
            ]
        })
    }

    fn github_answer(r: &HttpRequest) -> (u16, Value) {
        let u = &r.url;
        if u.contains("/search/issues") {
            let q = urlencoding::decode(u).unwrap().to_string();
            if q.contains("review-requested:dana") {
                return (
                    200,
                    json!({"items": [
                        {"number": 41, "title": "Add slips", "updated_at": "2026-11-12T00:00:00Z", "user": {"login": "sam"},
                         "repository_url": "https://api.github.com/repos/acme/web", "html_url": "https://github.com/acme/web/pull/41"},
                        {"number": 7, "title": "Not ours", "updated_at": "2026-11-16T00:00:00Z",
                         "repository_url": "https://api.github.com/repos/other/secret", "html_url": "https://github.com/other/secret/pull/7"}
                    ]}),
                );
            }
            if q.contains("author:dana") {
                return (
                    200,
                    json!({"items": [
                        {"number": 50, "title": "Speed up search", "updated_at": "2026-11-15T00:00:00Z",
                         "repository_url": "https://api.github.com/repos/acme/api", "html_url": "https://github.com/acme/api/pull/50"}
                    ]}),
                );
            }
            if q.contains("mentions:dana") {
                return (
                    200,
                    json!({"items": [
                        {"number": 60, "title": "Move the cron", "updated_at": "2026-11-15T00:00:00Z",
                         "repository_url": "https://api.github.com/repos/acme/web", "html_url": "https://github.com/acme/web/pull/60"}
                    ]}),
                );
            }
        }
        if u.ends_with("/repos/acme/api/pulls/50") {
            return (
                200,
                json!({"number": 50, "title": "Speed up search", "updated_at": "2026-11-15T00:00:00Z",
                "head": {"sha": "s50"}, "html_url": "https://github.com/acme/api/pull/50"}),
            );
        }
        if u.contains("/pulls/50/reviews") {
            return (
                200,
                json!([{"user": {"login": "amy"}, "state": "CHANGES_REQUESTED"}]),
            );
        }
        if u.contains("/commits/s50/check-runs") {
            return (
                200,
                json!({"check_runs": [{"name": "lint", "status": "completed", "conclusion": "failure"}]}),
            );
        }
        if u.contains("/issues/60/comments") {
            return (
                200,
                json!([{"user": {"login": "lee"}, "body": "@dana should this run hourly?",
                "created_at": "2026-11-15T08:00:00Z", "html_url": "https://github.com/acme/web/pull/60#issuecomment-1"}]),
            );
        }
        if u.contains("/pulls/60/comments") {
            return (200, json!([]));
        }
        (404, json!({}))
    }

    fn all() -> Vec<&'static str> {
        vec!["runs", "tickets", "pull_requests"]
    }

    pub fn me() -> Me {
        Me {
            email: ME.into(),
            github: Some("dana".into()),
            jira_id: None,
            is_admin: true,
        }
    }

    pub fn gh() -> GitHub {
        GitHub {
            auth: "Bearer t".into(),
            base: "https://gh.test".into(),
            held: None,
        }
    }

    /// The Jira app and GitHub behind one fake.
    pub fn http() -> crate::routes::chat_work_prs::tests::FakeGitHub {
        fake(|r| {
            if r.url.starts_with("https://jira-app.example.test") {
                let body: Value =
                    serde_json::from_slice(r.body.as_deref().unwrap_or(b"{}")).unwrap();
                assert_eq!(body["forge_secret"], "s3cret");
                if body["mine"]["account_id"] == JIRA_ME {
                    (200, jira_answer())
                } else {
                    (200, json!({"mine": {"found": true}, "issues": []}))
                }
            } else {
                github_answer(r)
            }
        })
    }

    async fn report_with(
        store: &FakeStore,
        h: &dyn Http,
        jira: Option<&JiraApp>,
        github: Option<GitHub>,
    ) -> Report {
        let repos = vec!["acme/web".to_string(), "acme/api".to_string()];
        let readable = all();
        let src = Sources {
            team: TEAM,
            store,
            http: h,
            jira,
            jira_off: "",
            github,
            repos: &repos,
            readable: &readable,
            budget: SOURCE_BUDGET,
        };
        gather(&src, me(), now()).await
    }

    fn find<'a>(r: &'a Report, what: &str) -> &'a Need {
        r.needs
            .iter()
            .find(|n| n.what.contains(what))
            .unwrap_or_else(|| panic!("no {what} in {:#?}", r.needs))
    }

    #[tokio::test]
    async fn every_source_is_read_and_ranked_by_urgency() {
        let s = store();
        let h = http();
        let a = app();
        let r = report_with(&s, &h, Some(&a), Some(gh())).await;
        // Runs: the question keeps its options; the outside change is named.
        let q = find(&r, "ABC-1 ");
        assert_eq!((q.urgency, q.kind), (Urgency::Now, "run"));
        assert!(q.detail.contains("3 tries; 5 tries"), "{}", q.detail);
        assert_eq!(q.link, "/runs/detail?id=01A");
        assert!(find(&r, "ABC-2 ").action.contains("outside the repository"));
        // Approvals for an admin.
        assert_eq!(find(&r, "restart_worker").urgency, Urgency::Now);
        // Jira: highest priority now, a mention today, low later, stale and
        // answered ones left out.
        assert_eq!(find(&r, "ABC-10").urgency, Urgency::Now);
        let m = find(&r, "ABC-13");
        assert_eq!(m.urgency, Urgency::Today);
        assert!(m.action.contains("Reply to Sam") && m.action.contains("64 MB"));
        assert_eq!(m.link, "https://team.atlassian.net/browse/ABC-13");
        assert_eq!(find(&r, "ABC-11").urgency, Urgency::Later);
        assert!(r
            .needs
            .iter()
            .all(|n| !n.what.contains("ABC-12") && !n.what.contains("ABC-14")));
        assert!(
            r.skipped.iter().any(|s| s.contains("1 assigned ticket")),
            "{:?}",
            r.skipped
        );
        assert!(
            r.skipped.iter().any(|s| s.contains("1 run")),
            "{:?}",
            r.skipped
        );
        // GitHub: a review waiting 4 days is now; own PR changes asked; an
        // unanswered mention.
        let rv = find(&r, "acme/web#41");
        assert_eq!(rv.urgency, Urgency::Now);
        assert!(rv.action.starts_with("Review it"));
        assert!(find(&r, "acme/api#50")
            .action
            .contains("changes amy asked for"));
        let mention = find(&r, "acme/web#60");
        assert!(mention.action.contains("Reply to lee") && mention.action.contains("hourly"));
        assert!(mention.link.ends_with("#issuecomment-1"));
        // Grouped: every Now before every Today before every Later.
        let order: Vec<Urgency> = r.needs.iter().map(|n| n.urgency).collect();
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(order, sorted);
        assert!(
            r.sources.iter().all(|s| matches!(s.read, Read::Ok(_))),
            "{:?}",
            r.sources
        );
        assert!(r.who.contains("GitHub @dana") && r.who.contains("Jira"));
    }

    #[tokio::test]
    async fn another_teams_records_never_show() {
        let s = store();
        let h = http();
        let a = app();
        let r = report_with(&s, &h, Some(&a), Some(gh())).await;
        let all_text = format!("{:?}", r.needs);
        for w in ["OTH-9", "drop_table", "other/secret", "09Z"] {
            assert!(!all_text.contains(w), "{w} leaked: {all_text}");
        }
        assert!(s.asked.lock().unwrap().iter().all(|t| t == TEAM));
        // The Jira account is this team's, not the other team's.
        assert!(h
            .sent
            .lock()
            .unwrap()
            .iter()
            .all(|r| !r.url.contains("other/secret")));
    }

    #[tokio::test]
    async fn a_failing_or_missing_source_is_named_and_the_rest_still_answer() {
        let s = FakeStore {
            fail_runs: true,
            ..store()
        };
        let down = fake(|r| {
            if r.url.starts_with("https://jira-app.example.test") {
                (500, json!({}))
            } else {
                github_answer(r)
            }
        });
        let a = app();
        let r = report_with(&s, &down, Some(&a), Some(gh())).await;
        let read = |n: &str| r.sources.iter().find(|s| s.name == n).unwrap().read.clone();
        assert!(matches!(read("Coderhelm's runs"), Read::Failed(e) if e.contains("AccessDenied")));
        assert!(matches!(read("Jira"), Read::Failed(e) if e.contains("500")));
        assert!(matches!(read("GitHub"), Read::Ok(_)));
        assert!(r.needs.iter().any(|n| n.what.contains("acme/web#41")));
        // Not connected, not linked: said plainly.
        let mut unlinked = me();
        unlinked.github = None;
        let repos = vec!["acme/web".to_string()];
        let readable = all();
        let src = Sources {
            team: TEAM,
            store: &s,
            http: &down,
            jira: None,
            jira_off: "",
            github: Some(gh()),
            repos: &repos,
            readable: &readable,
            budget: SOURCE_BUDGET,
        };
        let r = gather(&src, unlinked, now()).await;
        let (text, summary) = render(&r, &mut Default::default(), "2026-11-16 12:00 UTC");
        assert!(
            text.contains("Jira: not checked: Jira isn't connected to Coderhelm"),
            "{text}"
        );
        assert!(text.contains("isn't linked to a GitHub account"), "{text}");
        assert!(summary.contains("3 sources not read"), "{summary}");
        // Connected, but the app's lookup isn't registered: the answer names
        // that ability, never "not connected", and the runs still answer.
        let lacking = common::jira_connection::Gap::LookupNotRegistered.words();
        let src = Sources {
            jira_off: lacking,
            ..src
        };
        let r = gather(&src, me(), now()).await;
        let (text, _) = render(&r, &mut Default::default(), "2026-11-16 12:00 UTC");
        assert!(
            text.contains("Jira: not checked: Jira is connected, but"),
            "{text}"
        );
        assert!(!text.contains("Jira isn't connected"), "{text}");
        assert!(!ANSWER_SHAPE.contains("Jira isn't connected"));
        // An old Jira app (no `mine` in its answer) is named as such.
        let old = fake(|_| (200, json!({"issues": []})));
        let e = ask_jira_mine(&old, &app(), JIRA_ME).await.unwrap_err();
        assert!(e.contains("older version"), "{e}");
    }

    #[tokio::test]
    async fn a_source_that_hangs_is_cut_off_and_named() {
        struct Slow;
        #[async_trait::async_trait]
        impl Http for Slow {
            async fn send(
                &self,
                _: HttpRequest,
            ) -> Result<common::mcp_remote::http::HttpResponse, common::mcp_remote::http::HttpError>
            {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                unreachable!()
            }
        }
        let s = store();
        let a = app();
        let repos = vec!["acme/web".to_string()];
        let readable = all();
        let src = Sources {
            team: TEAM,
            store: &s,
            http: &Slow,
            jira: Some(&a),
            jira_off: "",
            github: Some(gh()),
            repos: &repos,
            readable: &readable,
            budget: Duration::from_millis(50),
        };
        let r = gather(&src, me(), now()).await;
        let jira = r.sources.iter().find(|s| s.name == "Jira").unwrap();
        assert!(matches!(&jira.read, Read::Failed(e) if e.contains("didn't answer within")));
        // What Coderhelm keeps still answers.
        assert!(r.needs.iter().any(|n| n.what.contains("ABC-1 ")));
    }

    #[test]
    fn ranking_puts_urgent_and_older_first_and_lists_a_link_once() {
        let n = |u: Urgency, rank: i64, link: &str| Need {
            urgency: u,
            rank,
            kind: "ticket",
            what: link.into(),
            action: String::new(),
            link: link.into(),
            since: String::new(),
            detail: String::new(),
            loc: String::new(),
        };
        let r = rank(vec![
            n(Urgency::Later, 99, "a"),
            n(Urgency::Today, 10, "b"),
            n(Urgency::Now, 5, "c"),
            n(Urgency::Today, 30, "d"),
            n(Urgency::Later, 1, "d"),
        ]);
        let links: Vec<&str> = r.iter().map(|x| x.link.as_str()).collect();
        assert_eq!(links, ["c", "d", "b", "a"]);
        assert_eq!(
            count_line(&r),
            "4 things need you: 1 now, 2 today, 1 when you can."
        );
        let many: Vec<Need> = (0..40)
            .map(|i| n(Urgency::Today, i, &format!("l{i}")))
            .collect();
        assert_eq!(rank(many).len(), MAX_ITEMS);
    }

    #[test]
    fn a_run_started_by_someone_else_waits_on_them_first() {
        let mut m = me();
        m.jira_id = Some(JIRA_ME.into());
        let theirs = waiting_run("02A", "ABC-5", "sam@example.com");
        let (n, _) = from_runs(std::slice::from_ref(&theirs), &m, &[], now());
        assert_eq!(n[0].urgency, Urgency::Today);
        assert!(n[0].what.contains("started by Sam"));
        // Unless the ticket is assigned to the asker.
        let (n, _) = from_runs(&[theirs], &m, &["ABC-5".into()], now());
        assert_eq!(n[0].urgency, Urgency::Now);
        // Coderhelm's open pull request on their own run.
        let mut done = with(
            run("02B", "completed", "ABC-6", "Add slips", "acme/web"),
            &[
                ("updated_at", "2026-11-15T00:00:00Z"),
                ("requester_email", ME),
            ],
        );
        done.insert(
            "pr_url".into(),
            AttributeValue::S("https://github.com/acme/web/pull/88".into()),
        );
        let (n, _) = from_runs(&[done], &m, &[], now());
        assert_eq!(n.len(), 1, "{n:?}");
        assert!(n[0].action.contains("merge Coderhelm's pull request"));
        assert_eq!(n[0].link, "https://github.com/acme/web/pull/88");
    }

    #[test]
    fn mentions_count_only_when_the_person_hasnt_answered() {
        let c = |who: &str, body: &str, at: &str| json!({"user": {"login": who}, "body": body, "created_at": at});
        let answered = vec![
            c("lee", "@dana thoughts?", "2026-11-15T08:00:00Z"),
            c("dana", "yes", "2026-11-15T09:00:00Z"),
        ];
        assert!(unanswered_github(&answered, "dana").is_none());
        let open = vec![
            c("dana", "first", "2026-11-14T08:00:00Z"),
            c("lee", "@Dana can you look?", "2026-11-15T08:00:00Z"),
        ];
        assert!(unanswered_github(&open, "dana").is_some());
        // "@dana-bot" is someone else.
        assert!(unanswered_github(
            &[c("lee", "@dana-bot ping", "2026-11-15T08:00:00Z")],
            "dana"
        )
        .is_none());
    }

    #[test]
    fn the_answer_text_is_grouped_counted_and_cites_every_item() {
        let r = Report {
            needs: rank(vec![
                Need {
                    urgency: Urgency::Now,
                    rank: 1,
                    kind: "run",
                    what: "ABC-1 Retry the export job".into(),
                    action: "Answer the run's question".into(),
                    link: "/runs/detail?id=01A".into(),
                    since: "2026-11-14T09:00:00Z".into(),
                    detail: "1. Which retry limit? Possible answers: 3 tries; 5 tries.".into(),
                    loc: "acme/web".into(),
                },
                Need {
                    urgency: Urgency::Later,
                    rank: 1,
                    kind: "ticket",
                    what: "ABC-11 Tidy the footer".into(),
                    action: "Start it".into(),
                    link: "https://team.atlassian.net/browse/ABC-11".into(),
                    since: String::new(),
                    detail: String::new(),
                    loc: "Jira".into(),
                },
            ]),
            sources: vec![SourceNote {
                name: "GitHub",
                read: Read::Failed("GitHub answered 502".into()),
            }],
            skipped: vec![],
            who: ME.into(),
        };
        let mut reg = common::knowledge::answer::Registry::default();
        let (text, summary) = render(&r, &mut reg, "2026-11-16 12:00 UTC");
        assert!(text.contains("Count line: 2 things need you: 1 now, 1 when you can."));
        let now_at = text.find("## Needs you now").unwrap();
        let later_at = text.find("## When you can").unwrap();
        assert!(now_at < later_at);
        assert!(text.contains("- [S1] [ABC-1 Retry the export job](/runs/detail?id=01A) | Answer the run's question | since Nov 14"));
        assert!(text.contains("Possible answers: 3 tries; 5 tries"));
        assert!(text.contains("GitHub: couldn't be read (GitHub answered 502)"));
        assert_eq!(reg.len(), 2);
        assert_eq!(
            reg.get("S1").unwrap().href.as_deref(),
            Some("/runs/detail?id=01A")
        );
        assert_eq!(
            reg.get("S2").unwrap().url.as_deref(),
            Some("https://team.atlassian.net/browse/ABC-11")
        );
        assert_eq!(summary, "2 items, 1 source not read");
    }
}
