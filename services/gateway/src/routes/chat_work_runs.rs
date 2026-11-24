//! Chat's runs lookup: the team's runs, current and past, found by ticket
//! key, pull request, repository, status, time or title words, newest first.
//! What each run did, where it stands, why it stopped, and its page.
//!
//! Matching is pure ([`score`], [`record`]) so tenancy and bounds are tested
//! without a table; [`find`] reads only the asker's team's partition.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::Value;

use super::chat_work::{clip, words_of, Found};
use crate::AppState;

pub type Item = HashMap<String, AttributeValue>;

/// Most runs one lookup reads, matched or not.
pub const SCAN: usize = 600;
/// Most runs one lookup returns.
pub const MAX_RESULTS: usize = 10;
/// Runs read in full (notes, why it stopped) among those returned.
pub const MAX_DETAILED: usize = 4;

/// Which runs, by where they stand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Group {
    #[default]
    Any,
    /// Working now, or waiting on an answer: "what's running".
    Active,
    Running,
    Waiting,
    Failed,
    Done,
    Stopped,
}

impl Group {
    pub fn parse(s: &str) -> Group {
        match s.trim().to_ascii_lowercase().as_str() {
            "active" => Group::Active,
            "running" => Group::Running,
            "waiting" => Group::Waiting,
            "failed" => Group::Failed,
            "done" => Group::Done,
            "stopped" => Group::Stopped,
            _ => Group::Any,
        }
    }

    fn holds(self, status: &str) -> bool {
        use common::run_status as rs;
        let waiting = [
            rs::NEEDS_INPUT,
            rs::NEEDS_OUTSIDE_CHANGE,
            rs::AWAITING_SETUP,
            rs::AWAITING_CHANGE,
        ];
        match self {
            // Archived runs show only when asked for by run id.
            Group::Any => status != rs::ARCHIVED,
            Group::Running => rs::IN_FLIGHT.contains(&status),
            Group::Waiting => waiting.contains(&status),
            Group::Active => rs::IN_FLIGHT.contains(&status) || waiting.contains(&status),
            Group::Failed => status == rs::FAILED,
            Group::Done => [
                rs::COMPLETED,
                rs::SUCCESS,
                rs::MERGED,
                rs::COVERED_BY_PR,
                rs::NO_CHANGE,
            ]
            .contains(&status),
            Group::Stopped => [rs::CANCELLED, rs::CLOSED, rs::RETRIED].contains(&status),
        }
    }
}

/// What the model asked for.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunAsk {
    pub run_id: Option<String>,
    /// A ticket key or id, lower case.
    pub ticket: Option<String>,
    pub pr: Option<u64>,
    /// `owner/name` or the short name, lower case.
    pub repo: Option<String>,
    pub group: Group,
    /// Words of a title, lower case.
    pub words: Vec<String>,
    /// Only runs started after this (RFC 3339).
    pub since: Option<String>,
}

impl RunAsk {
    pub fn from_input(input: &Value, now: chrono::DateTime<chrono::Utc>) -> RunAsk {
        let s = |k: &str| {
            input[k]
                .as_str()
                .map(str::trim)
                .filter(|v| !v.is_empty() && v.len() <= 200)
                .map(str::to_string)
        };
        let pr = input["pr"]
            .as_u64()
            .or_else(|| s("pr").and_then(|p| p.trim_start_matches('#').parse().ok()));
        let since = input["days"]
            .as_u64()
            .filter(|d| *d > 0)
            .map(|d| (now - chrono::Duration::days(d.min(365) as i64)).to_rfc3339());
        let mut ask = RunAsk {
            run_id: s("run_id").filter(|id| super::brain::valid_run_id(id)),
            ticket: s("ticket").map(|t| t.to_lowercase()),
            pr,
            repo: s("repo").map(|r| r.to_lowercase()),
            group: s("status").map(|g| Group::parse(&g)).unwrap_or_default(),
            words: Vec::new(),
            since,
        };
        // A query that is only a pull request number ("#609") or a ticket
        // key ("SEC-1A2B") asks by that, not by title words: neither is in
        // a run's title.
        if let Some(q) = s("query") {
            match query_names(&q) {
                Named::Pr(n) if ask.pr.is_none() => ask.pr = Some(n),
                Named::Ticket(k) if ask.ticket.is_none() => ask.ticket = Some(k),
                Named::Words | Named::Pr(_) | Named::Ticket(_) => ask.words = words_of(&q),
            }
        }
        ask
    }
}

/// What a lookup's free text names on its own.
#[derive(Debug, PartialEq)]
enum Named {
    Pr(u64),
    /// Lower case.
    Ticket(String),
    Words,
}

fn query_names(q: &str) -> Named {
    let q = q.trim();
    let digits = q.strip_prefix('#').unwrap_or(q);
    if !digits.is_empty() && digits.len() <= 9 && digits.bytes().all(|b| b.is_ascii_digit()) {
        if let Ok(n) = digits.parse() {
            return Named::Pr(n);
        }
    }
    if ticket_key(q) {
        return Named::Ticket(q.to_lowercase());
    }
    Named::Words
}

/// A ticket key as runs keep it: Jira's (`ABC-123`) and Coderhelm's own
/// (`SEC-25142555683B`, `ALERT-3DCB8D72`, `E2E-FIX-1a2b`): letters first,
/// then dash-joined parts with at least one digit after the first dash.
pub fn ticket_key(k: &str) -> bool {
    let Some((head, rest)) = k.split_once('-') else {
        return false;
    };
    (2..=64).contains(&k.len())
        && head.bytes().next().is_some_and(|b| b.is_ascii_alphabetic())
        && head.bytes().all(|b| b.is_ascii_alphanumeric())
        && head.bytes().any(|b| b.is_ascii_uppercase())
        && !rest.is_empty()
        && rest
            .split('-')
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_alphanumeric()))
        && rest.bytes().any(|b| b.is_ascii_digit())
}

fn st<'a>(item: &'a Item, k: &str) -> &'a str {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .unwrap_or("")
}

fn pr_numbers(item: &Item) -> Vec<u64> {
    let mut out: Vec<u64> = item
        .get("pr_numbers")
        .and_then(|v| v.as_l().ok())
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_n().ok()?.parse().ok())
        .collect();
    out.extend(
        item.get("pr_number")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok()),
    );
    out.extend(common::run_prs::from_item(item).iter().map(|p| p.pr_number));
    out
}

/// How well a run matches (higher first); `None` when it doesn't.
pub fn score(item: &Item, ask: &RunAsk) -> Option<usize> {
    let status = st(item, "status");
    if let Some(id) = &ask.run_id {
        return (st(item, "run_id") == id).then_some(100);
    }
    if !ask.group.holds(status) {
        return None;
    }
    let repo = st(item, "repo").to_lowercase();
    if let Some(r) = &ask.repo {
        let short = repo.rsplit('/').next().unwrap_or(&repo);
        if repo != *r && short != r.rsplit('/').next().unwrap_or(r) {
            return None;
        }
    }
    let title = st(item, "title").to_lowercase();
    let ticket = st(item, "ticket_id").to_lowercase();
    if let Some(t) = &ask.ticket {
        // A key in the ticket field, or named in the title.
        if ticket != *t && !title.contains(t.as_str()) {
            return None;
        }
    }
    if let Some(n) = ask.pr {
        if !pr_numbers(item).contains(&n) {
            return None;
        }
    }
    if let Some(since) = &ask.since {
        if st(item, "created_at") < since.as_str() {
            return None;
        }
    }
    if ask.words.is_empty() {
        return Some(1);
    }
    let hay = format!("{title} {ticket}");
    let hits = ask
        .words
        .iter()
        .filter(|w| hay.contains(w.as_str()))
        .count();
    // Most of a title's words: a ticket's title and its run's differ a little.
    (hits > 0 && hits * 3 >= ask.words.len() * 2).then_some(hits)
}

/// The best matches among `items` (read newest first), best and newest first.
pub fn best<'a>(items: &'a [Item], ask: &RunAsk, max: usize) -> Vec<&'a Item> {
    let mut scored: Vec<(usize, &str, &Item)> = items
        .iter()
        .filter_map(|i| score(i, ask).map(|s| (s, st(i, "run_id"), i)))
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(a.1)));
    scored.into_iter().take(max).map(|(_, _, i)| i).collect()
}

/// A run's status in plain words.
pub fn status_words(status: &str) -> &'static str {
    use common::run_status as rs;
    match status {
        rs::PENDING | rs::QUEUED => "queued, about to start",
        rs::RUNNING => "working now",
        rs::AWAITING_CI => "waiting on the pull request's checks",
        rs::NEEDS_INPUT => "waiting on an answer from the team",
        rs::NEEDS_OUTSIDE_CHANGE => "waiting on a change outside its repository",
        rs::COVERED_BY_PR => "a pull request already does the work",
        rs::NO_CHANGE => "finished; the code already does it, so no change was needed",
        rs::COMPLETED | rs::SUCCESS => "finished; its pull request is open or done",
        rs::MERGED => "finished; its pull request was merged",
        rs::FAILED => "stopped with an error",
        rs::CANCELLED => "cancelled",
        rs::CLOSED => "closed by a person",
        rs::RETRIED => "replaced by a newer run",
        rs::ARCHIVED => "archived",
        _ => "unknown",
    }
}

/// One run as a citable record. `full`: the run's whole item (notes, why it
/// stopped); a listed run carries only what the list reads.
pub fn record(item: &Item) -> Found {
    let id = st(item, "run_id");
    let status = st(item, "status");
    let title = st(item, "title");
    let repo = st(item, "repo");
    let mut lines = vec![format!("Run {id}: {status} ({})", status_words(status))];
    fn field(lines: &mut Vec<String>, label: &str, v: &str) {
        if !v.trim().is_empty() {
            lines.push(format!("{label}: {}", clip(v, 600)));
        }
    }
    field(&mut lines, "Title", title);
    let ticket = st(item, "ticket_id");
    if !ticket.is_empty() {
        let src = st(item, "ticket_source");
        field(
            &mut lines,
            "Ticket",
            &if src.is_empty() {
                ticket.to_string()
            } else {
                format!("{ticket} ({src})")
            },
        );
    }
    field(&mut lines, "Repository", repo);
    let prs = common::run_prs::from_item(item);
    if prs.is_empty() {
        field(&mut lines, "Pull request", st(item, "pr_url"));
    }
    for p in &prs {
        let state = if p.outcome.is_empty() {
            "open"
        } else {
            p.outcome.as_str()
        };
        lines.push(format!("Pull request: {} ({state})", p.url));
    }
    field(&mut lines, "Current step", st(item, "current_pass"));
    field(&mut lines, "Started", st(item, "created_at"));
    field(&mut lines, "Last update", st(item, "updated_at"));
    let why = [st(item, "error_message"), st(item, "error")]
        .into_iter()
        .find(|e| !e.trim().is_empty())
        .unwrap_or("");
    field(&mut lines, "Why it stopped", why);
    field(&mut lines, "How it stopped", st(item, "stop_kind"));
    if status == common::run_status::CLOSED {
        let by = [st(item, "closed_by_name"), st(item, "closed_by")]
            .into_iter()
            .find(|b| !b.is_empty())
            .unwrap_or("a person");
        let reason = st(item, "close_reason");
        lines.push(format!(
            "Closed by {by}{}{}",
            if reason.is_empty() { "" } else { ": " },
            clip(reason, 300)
        ));
    }
    let asked: Option<common::run_questions::Asked> = item
        .get(common::run_questions::FIELD_QUESTIONS)
        .and_then(|v| v.as_s().ok())
        .and_then(|j| serde_json::from_str(j).ok());
    if let Some(a) = asked.filter(|a| !a.questions.is_empty()) {
        lines.push(format!(
            "Waiting on answers to:\n{}",
            clip(&common::run_questions::plain(&a.questions), 800)
        ));
    }
    let notes: Vec<String> = item
        .get("progress_notes")
        .and_then(|v| v.as_l().ok())
        .into_iter()
        .flatten()
        .filter_map(|n| n.as_m().ok())
        .map(|m| {
            let g = |f: &str| {
                m.get(f)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            format!("{}: {}", g("timestamp"), clip(&g("message"), 240))
        })
        .collect();
    if !notes.is_empty() {
        let start = notes.len().saturating_sub(3);
        lines.push(format!("Latest notes:\n{}", notes[start..].join("\n")));
    }
    let href = format!("/runs/detail?id={}", urlencoding::encode(id));
    lines.push(format!("Run page: {href}"));
    let as_of = [st(item, "updated_at"), st(item, "created_at")]
        .into_iter()
        .find(|t| !t.is_empty())
        .unwrap_or("")
        .to_string();
    Found {
        kind: "run",
        title: if title.is_empty() {
            format!("Run {id}")
        } else {
            format!("Run: {}", clip(title, 100))
        },
        loc: repo.to_string(),
        as_of,
        passage: lines.join("\n"),
        href: Some(href),
        url: None,
    }
}

/// The attributes a lookup reads for every run (the list's, and a few more).
fn projection() -> (String, HashMap<String, String>) {
    let extra = ["updated_at", "prs", "error_message", "stop_kind"];
    let names: HashMap<String, String> = super::run_search::LIST_ATTRS
        .iter()
        .chain(extra.iter())
        .enumerate()
        .map(|(i, a)| (format!("#a{i}"), a.to_string()))
        .collect();
    let mut keys: Vec<&String> = names.keys().collect();
    keys.sort();
    let p = keys
        .iter()
        .map(|k| k.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    (p, names)
}

/// A DynamoDB failure with its cause (AccessDenied, a missing index), not
/// the SDK's bare "service error".
fn why<E, R>(what: &str, e: aws_sdk_dynamodb::error::SdkError<E, R>) -> String
where
    E: std::error::Error + 'static,
    R: std::fmt::Debug,
{
    format!(
        "runs: couldn't {what}: {}",
        aws_sdk_dynamodb::error::DisplayErrorContext(&e)
    )
}

async fn get_run(state: &AppState, team_id: &str, run_id: &str) -> Result<Option<Item>, String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .send()
        .await
        .map(|r| r.item().cloned())
        .map_err(|e| why("read a run", e))
}

/// The team's listed runs that match, best and newest first (what the list
/// reads of each). Only `team_id`'s partition is read.
async fn matching(state: &AppState, team_id: &str, ask: &RunAsk) -> Result<Vec<Item>, String> {
    let (projection, names) = projection();
    let mut items: Vec<Item> = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .key_condition_expression("team_id = :tid")
            .expression_attribute_values(":tid", AttributeValue::S(team_id.to_string()))
            .projection_expression(&projection)
            .set_expression_attribute_names(Some(names.clone()))
            .scan_index_forward(false)
            .limit((SCAN - items.len()).min(200) as i32)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| why("list the runs", e))?;
        items.extend(r.items().iter().cloned());
        // An unfiltered "what's running" stops at the first page full of matches.
        let enough = ask.words.is_empty() && best(&items, ask, MAX_RESULTS).len() >= MAX_RESULTS;
        match r.last_evaluated_key() {
            Some(k) if !k.is_empty() && items.len() < SCAN && !enough => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(best(&items, ask, MAX_RESULTS)
        .into_iter()
        .cloned()
        .collect())
}

/// The team's runs that match, newest first, the first few read in full.
/// Only `team_id`'s partition is read.
pub async fn find(state: &AppState, team_id: &str, ask: &RunAsk) -> Result<Vec<Found>, String> {
    if let Some(id) = &ask.run_id {
        return Ok(get_run(state, team_id, id)
            .await?
            .iter()
            .map(record)
            .collect());
    }
    let listed = Box::pin(matching(state, team_id, ask)).await?;
    let mut out = Vec::new();
    for (n, listed) in listed.iter().enumerate() {
        let full = if n < MAX_DETAILED {
            get_run(state, team_id, st(listed, "run_id"))
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        out.push(record(full.as_ref().unwrap_or(listed)));
    }
    Ok(out)
}

/// What Coderhelm's runs did for a team over a window: counted from the
/// runs themselves, for "what does Coderhelm already do for us".
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Tally {
    pub days: u32,
    pub started: usize,
    pub done: usize,
    pub running: usize,
    pub waiting: usize,
    pub failed: usize,
    pub stopped: usize,
    /// Runs that opened a pull request.
    pub with_pr: usize,
    /// Repositories the runs worked in, most runs first.
    pub repos: Vec<(String, usize)>,
    /// When the newest run started.
    pub latest: String,
    /// More runs may lie past what was read.
    pub capped: bool,
}

/// [`Tally`] from listed runs (newest first), counting those started on or
/// after `since` (RFC 3339).
pub fn tally_of(items: &[Item], since: &str, days: u32, capped: bool) -> Tally {
    let mut t = Tally {
        days,
        capped,
        ..Default::default()
    };
    let mut repos: HashMap<String, usize> = HashMap::new();
    for i in items {
        let status = st(i, "status");
        let created = st(i, "created_at");
        if status == common::run_status::ARCHIVED || created < since {
            continue;
        }
        t.started += 1;
        if t.latest.as_str() < created {
            t.latest = created.to_string();
        }
        if Group::Done.holds(status) {
            t.done += 1;
        } else if Group::Running.holds(status) {
            t.running += 1;
        } else if Group::Waiting.holds(status) {
            t.waiting += 1;
        } else if Group::Failed.holds(status) {
            t.failed += 1;
        } else if Group::Stopped.holds(status) {
            t.stopped += 1;
        }
        if !pr_numbers(i).is_empty() || !st(i, "pr_url").is_empty() {
            t.with_pr += 1;
        }
        let repo = st(i, "repo");
        if !repo.is_empty() {
            *repos.entry(repo.to_string()).or_default() += 1;
        }
    }
    let mut repos: Vec<(String, usize)> = repos.into_iter().collect();
    repos.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    repos.truncate(8);
    t.repos = repos;
    t
}

/// The team's runs over the last `days`, counted. Only `team_id`'s
/// partition is read, newest first, at most [`SCAN`] runs.
pub async fn tally(state: &AppState, team_id: &str, days: u32) -> Result<Tally, String> {
    let since = (chrono::Utc::now() - chrono::Duration::days(days as i64)).to_rfc3339();
    let (projection, names) = projection();
    let mut items: Vec<Item> = Vec::new();
    let mut start: Option<Item> = None;
    let mut capped = false;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .key_condition_expression("team_id = :tid")
            .expression_attribute_values(":tid", AttributeValue::S(team_id.to_string()))
            .projection_expression(&projection)
            .set_expression_attribute_names(Some(names.clone()))
            .scan_index_forward(false)
            .limit((SCAN - items.len()).min(200) as i32)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| why("list the runs", e))?;
        items.extend(r.items().iter().cloned());
        match r.last_evaluated_key() {
            Some(k) if !k.is_empty() && items.len() < SCAN => start = Some(k.clone()),
            Some(k) if !k.is_empty() => {
                capped = true;
                break;
            }
            _ => break,
        }
    }
    Ok(tally_of(&items, &since, days, capped))
}

/// The pull requests the matching runs opened, as `(owner/name, number)`,
/// best match first: a title search finds its pull request through the run
/// that opened it, whatever GitHub's search makes of the title.
pub async fn pull_requests(
    state: &AppState,
    team_id: &str,
    ask: &RunAsk,
) -> Result<Vec<(String, u64)>, String> {
    let items = Box::pin(matching(state, team_id, ask)).await?;
    Ok(prs_of(&items))
}

/// [`pull_requests`] from listed runs.
pub fn prs_of(items: &[Item]) -> Vec<(String, u64)> {
    let mut out: Vec<(String, u64)> = Vec::new();
    for i in items {
        let repo = st(i, "repo");
        if repo.is_empty() {
            continue;
        }
        let mut nums = pr_numbers(i);
        nums.sort_unstable_by(|a, b| b.cmp(a));
        for n in nums {
            if !out.iter().any(|(r, m)| r == repo && *m == n) {
                out.push((repo.to_string(), n));
            }
        }
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn run(id: &str, status: &str, ticket: &str, title: &str, repo: &str) -> Item {
        HashMap::from([
            ("run_id".to_string(), AttributeValue::S(id.into())),
            ("status".to_string(), AttributeValue::S(status.into())),
            (
                "ticket_source".to_string(),
                AttributeValue::S("jira".into()),
            ),
            ("ticket_id".to_string(), AttributeValue::S(ticket.into())),
            ("title".to_string(), AttributeValue::S(title.into())),
            ("repo".to_string(), AttributeValue::S(repo.into())),
            (
                "created_at".to_string(),
                AttributeValue::S(format!("2026-11-{:02}T10:00:00Z", id.len().min(28))),
            ),
        ])
    }

    fn ask(v: Value) -> RunAsk {
        RunAsk::from_input(&v, chrono::Utc::now())
    }

    #[test]
    fn the_tally_counts_the_window_by_where_each_run_stands() {
        let mut with_pr = run("01AAAAAAAAAAAAAAAAAAAA", "merged", "A-1", "x", "acme/web");
        with_pr.insert(
            "pr_url".into(),
            AttributeValue::S("https://github.com/acme/web/pull/3".into()),
        );
        let items = vec![
            with_pr,
            run("01AAAAAAAAAAAAAAAAAAAB", "running", "A-2", "y", "acme/web"),
            run(
                "01AAAAAAAAAAAAAAAAAAAC",
                "needs_input",
                "A-3",
                "z",
                "acme/api",
            ),
            run("01AAAAAAAAAAAAAAAAAAAD", "failed", "A-4", "w", "acme/api"),
            run("01AAAAAAAAAAAAAAAAAAAE", "archived", "A-5", "v", "acme/api"),
            // Started before the window.
            run("01A", "merged", "A-6", "u", "acme/old"),
        ];
        let t = tally_of(&items, "2026-11-10T00:00:00Z", 30, false);
        assert_eq!(
            (t.started, t.done, t.running, t.waiting, t.failed, t.with_pr),
            (4, 1, 1, 1, 1, 1)
        );
        assert_eq!(
            t.repos,
            [("acme/api".to_string(), 2), ("acme/web".to_string(), 2)]
        );
        assert!(!t.latest.is_empty() && !t.capped);
    }

    #[test]
    fn a_title_finds_its_run_even_when_the_words_differ_a_little() {
        let items = vec![
            run(
                "01B",
                "running",
                "ABC-7",
                "Embed the booking portal on the signup pages",
                "acme/web",
            ),
            run(
                "01A",
                "completed",
                "ABC-3",
                "Fix the login redirect",
                "acme/web",
            ),
        ];
        let a = ask(serde_json::json!({"query": "Embed booking portal on signup page"}));
        let got = best(&items, &a, 10);
        assert_eq!(got.len(), 1);
        assert_eq!(st(got[0], "run_id"), "01B");
    }

    #[test]
    fn what_is_running_keeps_only_active_runs_newest_first() {
        let items = vec![
            run("03", "running", "A-3", "x", "acme/web"),
            run("02", "needs_input", "A-2", "y", "acme/web"),
            run("01", "failed", "A-1", "z", "acme/web"),
            run("00", "archived", "A-0", "w", "acme/web"),
        ];
        let got = best(&items, &ask(serde_json::json!({"status": "active"})), 10);
        let ids: Vec<&str> = got.iter().map(|i| st(i, "run_id")).collect();
        assert_eq!(ids, ["03", "02"]);
        // Archived runs never show in a plain search.
        let all = best(&items, &ask(serde_json::json!({})), 10);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn a_ticket_key_a_pr_and_a_repo_narrow_the_runs() {
        let mut with_pr = run("02", "completed", "ABC-12", "Add slips", "acme/api");
        with_pr.insert("pr_number".into(), AttributeValue::N("41".into()));
        let items = vec![with_pr, run("01", "failed", "ABC-120", "Other", "acme/web")];
        assert_eq!(
            best(&items, &ask(serde_json::json!({"ticket": "abc-12"})), 10).len(),
            1
        );
        assert_eq!(
            best(&items, &ask(serde_json::json!({"pr": 41})), 10).len(),
            1
        );
        assert_eq!(
            best(&items, &ask(serde_json::json!({"repo": "web"})), 10).len(),
            1
        );
        assert_eq!(
            best(&items, &ask(serde_json::json!({"repo": "acme/api"})), 10).len(),
            1
        );
    }

    #[test]
    fn a_number_or_a_key_in_the_query_asks_by_it() {
        let now = chrono::Utc::now();
        let a = RunAsk::from_input(&serde_json::json!({"query": "#609"}), now);
        assert_eq!((a.pr, a.words.len()), (Some(609), 0));
        let a = RunAsk::from_input(&serde_json::json!({"query": "SEC-25142555683B"}), now);
        assert_eq!(a.ticket.as_deref(), Some("sec-25142555683b"));
        assert!(a.words.is_empty());
        // Words stay words; a number given twice keeps the field's.
        let a = RunAsk::from_input(
            &serde_json::json!({"query": "security updates gangway-api"}),
            now,
        );
        assert_eq!(a.words, ["security", "updates", "gangway-api"]);
        let a = RunAsk::from_input(&serde_json::json!({"query": "#5", "pr": 7}), now);
        assert_eq!(a.pr, Some(7));
        let items = vec![
            run(
                "01B",
                "running",
                "SEC-25142555683B",
                "fix(deps): security updates for api",
                "acme/api",
            ),
            run("01A", "retried", "ABC-1", "Other", "acme/web"),
        ];
        let a = RunAsk::from_input(&serde_json::json!({"query": "SEC-25142555683B"}), now);
        let got = best(&items, &a, 10);
        assert_eq!(got.len(), 1);
        assert_eq!(st(got[0], "run_id"), "01B");
    }

    #[test]
    fn ticket_keys_include_coderhelms_own() {
        for k in [
            "ABC-123",
            "SEC-25142555683B",
            "ALERT-3DCB8D72",
            "E2E-FIX-9ebf132c",
            "GH-12",
        ] {
            assert!(ticket_key(k), "{k}");
        }
        for k in [
            "gangway-api",
            "fix-deps",
            "SEC-",
            "-1",
            "SEC-ABC",
            "a b-1",
            "#609",
        ] {
            assert!(!ticket_key(k), "{k}");
        }
    }

    #[test]
    fn the_pull_requests_runs_opened_are_listed_once_newest_first() {
        let mut a = run("01B", "running", "SEC-1A", "t", "acme/api");
        a.insert("pr_number".into(), AttributeValue::N("609".into()));
        a.insert(
            "pr_numbers".into(),
            AttributeValue::L(vec![
                AttributeValue::N("602".into()),
                AttributeValue::N("609".into()),
            ]),
        );
        let b = run("01A", "retried", "SEC-1A", "t", "acme/api");
        let none = run("01C", "failed", "SEC-1A", "t", "");
        let got = prs_of(&[a, b, none]);
        assert_eq!(
            got,
            [("acme/api".to_string(), 609), ("acme/api".to_string(), 602)]
        );
    }

    #[test]
    fn results_are_bounded() {
        let items: Vec<Item> = (0..50)
            .map(|n| run(&format!("{n:03}"), "completed", "K-1", "t", "acme/web"))
            .collect();
        let got = best(&items, &ask(serde_json::json!({})), MAX_RESULTS);
        assert_eq!(got.len(), MAX_RESULTS);
        assert_eq!(st(got[0], "run_id"), "049");
    }

    #[test]
    fn a_run_record_says_why_it_stopped_and_links_its_page() {
        let mut item = run("01X", "failed", "ABC-9", "Embed the portal", "acme/web");
        item.insert(
            "error_message".into(),
            AttributeValue::S("The build failed: a missing package".into()),
        );
        let f = record(&item);
        assert_eq!(f.kind, "run");
        assert!(
            f.passage.contains("Why it stopped: The build failed"),
            "{}",
            f.passage
        );
        assert!(f.passage.contains("stopped with an error"));
        assert_eq!(f.href.as_deref(), Some("/runs/detail?id=01X"));
        assert!(f.passage.contains("Ticket: ABC-9 (jira)"));
    }
}
