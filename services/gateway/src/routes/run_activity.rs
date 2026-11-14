//! GET /api/runs/:run_id/activity: a run's story in time order, for the
//! Activity tab on the run page.
//!
//! Nothing new is recorded for it. Each entry is read from a record the
//! platform already keeps:
//!
//! - the run record: when it started and from where, each step it began,
//!   the notes it wrote, the questions it asked (and the comment that asked
//!   them), the answers people gave, CI fixes it pushed, a CI failure handed
//!   to a person, a fix round that didn't finish, a GitHub limit wait, a
//!   close, a continuation and where it stands now;
//! - the step traces: each step's end, how long it took, or why it stopped;
//! - approval cards the run's agents asked for, and who answered them;
//! - questions the run put to the team's own agents;
//! - CoderHelm's review verdicts on the run's pull requests;
//! - for admins, the events and messages tied to those pull requests
//!   (the deliveries log).
//!
//! Every read is bounded. The entries are merged oldest first and served
//! newest last, a page at a time from the newest end: `before` (an entry's
//! `id`) asks for the page before it, and `earlier` in the answer is the
//! cursor for the next older page, null when there is none.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use common::agent_policy::approvals::{Approval, Status as ApprovalStatus};
use common::deliveries::{Delivery, Direction};
use common::run_prs::RunPr;
use common::skills::AgentAsk;
use serde::Serialize;
use serde_json::{json, Value};
use tracing::warn;

use super::run_search::Item;
use super::run_trigger::{RunTrigger, TriggerKind};
use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// A page holds this many entries unless asked for fewer.
pub const PAGE: usize = 200;
/// The most a page may hold.
pub const MAX_PAGE: usize = 500;
/// The most entries one run's story keeps (the newest win).
pub const MAX_ENTRIES: usize = 2_000;
/// The most pull requests of a run whose reviews and deliveries are read.
const MAX_PRS: usize = 3;
/// The most review records read per pull request.
const REVIEWS_PER_PR: i32 = 25;
/// The most deliveries kept for one run.
const MAX_DELIVERIES: usize = 200;
/// A line is clipped to this many characters.
const MAX_TEXT: usize = 240;

/// Who did it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Actor {
    Coderhelm,
    Person,
    Github,
    Jira,
}

/// How it went, for the dot beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Tone {
    Neutral,
    Good,
    Problem,
}

/// One thing that happened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Entry {
    /// Unique within the run and in time order: the time, then where it was
    /// read from. The page cursor.
    pub id: String,
    /// When (RFC 3339, UTC, milliseconds).
    pub at: String,
    pub actor: Actor,
    /// The person, when one did it and their name is known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub who: Option<String>,
    /// What kind of thing: `started`, `step`, `step_done`, `step_failed`,
    /// `note`, `asked`, `answered`, `approval`, `approval_answered`,
    /// `agent_ask`, `review`, `pushed`, `handed_off`, `merged`, `round`,
    /// `wait`, `closed`, `continued`, `status`, `delivery`.
    pub kind: &'static str,
    /// One plain line.
    pub text: String,
    /// Where to see it: a full URL, or a dashboard path starting with `/`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    pub tone: Tone,
}

/// The time as one sortable form (UTC, milliseconds, `Z`), or None when it
/// isn't a time.
pub fn norm(at: &str) -> Option<String> {
    chrono::DateTime::parse_from_rfc3339(at.trim())
        .ok()
        .map(|t| {
            t.with_timezone(&chrono::Utc)
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        })
}

/// The first line, clipped.
fn line(s: &str) -> String {
    let first = s.trim().lines().next().unwrap_or("").trim();
    if first.chars().count() <= MAX_TEXT {
        return first.to_string();
    }
    let cut: String = first.chars().take(MAX_TEXT - 1).collect();
    format!("{}…", cut.trim_end())
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

/// Words people read for a step name the worker records (`ci_fix:2`).
pub fn step_label(raw: &str) -> String {
    let (base, n) = raw.split_once(':').unwrap_or((raw, ""));
    let word = match base {
        "triage" => "Triage".to_string(),
        "plan" => "Plan".to_string(),
        "implement" => "Build".to_string(),
        "test" => "Test".to_string(),
        "security" => "Security".to_string(),
        "pr" => "Pull request".to_string(),
        "review" => "Checks".to_string(),
        "feedback" => "Feedback".to_string(),
        "ci_fix" => "CI fix".to_string(),
        other => {
            let spaced = other.replace('_', " ");
            let mut c = spaced.chars();
            match c.next() {
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                None => "A step".to_string(),
            }
        }
    };
    if n.is_empty() || n == "1" {
        word
    } else {
        format!("{word} #{n}")
    }
}

fn duration(ms: u64) -> String {
    let s = ms / 1000;
    if s < 60 {
        format!("{}s", s.max(1))
    } else if s < 3600 {
        format!("{}m {}s", s / 60, s % 60)
    } else {
        format!("{}h {}m", s / 3600, (s % 3600) / 60)
    }
}

/// Builds entries for one source, numbering them so two at the same time
/// stay apart.
struct Out<'a> {
    source: &'a str,
    list: Vec<Entry>,
}

impl<'a> Out<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            list: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        at: &str,
        actor: Actor,
        who: Option<String>,
        kind: &'static str,
        text: String,
        link: Option<String>,
        tone: Tone,
    ) {
        let Some(at) = norm(at) else { return };
        if text.is_empty() {
            return;
        }
        let id = format!("{at}#{}#{:04}", self.source, self.list.len());
        self.list.push(Entry {
            id,
            at,
            actor,
            who: who.filter(|w| !w.trim().is_empty()),
            kind,
            text,
            link: link.filter(|l| !l.is_empty()),
            tone,
        });
    }
}

fn s<'a>(item: &'a Item, key: &str) -> &'a str {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .unwrap_or("")
}

fn strings(item: &Item, key: &str) -> Vec<String> {
    item.get(key)
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default()
}

fn maps<'a>(item: &'a Item, key: &str) -> Vec<&'a HashMap<String, AttributeValue>> {
    item.get(key)
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|v| v.as_m().ok()).collect())
        .unwrap_or_default()
}

fn field<'a>(m: &'a HashMap<String, AttributeValue>, key: &str) -> &'a str {
    m.get(key)
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .unwrap_or("")
}

/// Who started a run, from how it was started.
fn starter(t: &RunTrigger) -> Actor {
    match t.kind {
        TriggerKind::Jira => Actor::Jira,
        TriggerKind::Github => Actor::Github,
        _ if t.by.is_some() => Actor::Person,
        _ => Actor::Coderhelm,
    }
}

/// What the run record says happened.
pub fn from_run(item: &Item, trigger: &RunTrigger, run_id: &str) -> Vec<Entry> {
    let mut o = Out::new("run");
    let pr_url = s(item, "pr_url").to_string();
    let pr_link = (!pr_url.is_empty()).then(|| pr_url.clone());

    // Started, and from where.
    let from = match &trigger.r#ref {
        Some(r) if !r.is_empty() => format!("{} {r}", trigger.label),
        _ => trigger.label.clone(),
    };
    o.push(
        s(item, "created_at"),
        starter(trigger),
        trigger.by.clone(),
        "started",
        if from.is_empty() {
            "Run started".to_string()
        } else {
            line(&format!("Run started from {from}"))
        },
        trigger.ref_url.clone(),
        Tone::Neutral,
    );

    for p in maps(item, "pass_history") {
        let pass = field(p, "pass");
        if pass.is_empty() {
            continue;
        }
        o.push(
            field(p, "started_at"),
            Actor::Coderhelm,
            None,
            "step",
            format!("Started {}", step_label(pass).to_lowercase_first()),
            None,
            Tone::Neutral,
        );
    }

    for n in maps(item, "progress_notes") {
        o.push(
            field(n, "timestamp"),
            Actor::Coderhelm,
            None,
            "note",
            line(field(n, "message")),
            None,
            Tone::Neutral,
        );
    }

    if let Some(asked) = super::api::run_asked(item) {
        let text = match asked.questions.as_slice() {
            [] => String::new(),
            [q] => line(&format!("Asked: {}", q.question)),
            qs => format!(
                "Asked {} on the ticket",
                plural(qs.len(), "question", "questions")
            ),
        };
        o.push(
            &asked.asked_at,
            Actor::Coderhelm,
            None,
            "asked",
            text,
            asked.comment_url.clone(),
            Tone::Neutral,
        );
    }
    let answers_url = s(item, "answers_url").to_string();
    for a in super::api::run_answers(item) {
        o.push(
            &a.answered_at,
            Actor::Person,
            Some(a.author.clone()),
            "answered",
            line(&format!("Answered \"{}\": {}", a.question, a.answer)),
            Some(answers_url.clone()),
            Tone::Neutral,
        );
    }

    for at in strings(item, "ci_fixes_at") {
        o.push(
            &at,
            Actor::Coderhelm,
            None,
            "pushed",
            "Pushed a fix for the failing checks".to_string(),
            pr_link.clone(),
            Tone::Neutral,
        );
    }
    let handed = s(item, "handed_off_at");
    if !handed.is_empty() {
        o.push(
            handed,
            Actor::Coderhelm,
            None,
            "handed_off",
            "Handed the failing checks to a person".to_string(),
            pr_link.clone(),
            Tone::Problem,
        );
    }
    let merged = s(item, "merged_at");
    if !merged.is_empty() {
        o.push(
            merged,
            Actor::Github,
            None,
            "merged",
            "Pull request merged".to_string(),
            pr_link.clone(),
            Tone::Good,
        );
    }

    if let Some(inc) = common::run_round::Incomplete::from_item(item) {
        let what = inc.kind.map(|k| k.label()).unwrap_or("fix round");
        let pushed = match inc.pushed.len() {
            0 => String::new(),
            n => format!(" ({} pushed)", plural(n, "commit", "commits")),
        };
        let why = if inc.reason.is_empty() {
            String::new()
        } else {
            format!(": {}", inc.reason)
        };
        o.push(
            &inc.at,
            Actor::Coderhelm,
            None,
            "round",
            line(&format!("The {what} didn't finish{pushed}{why}")),
            pr_link.clone(),
            Tone::Problem,
        );
    }

    let updated = s(item, "updated_at");
    let wait = s(item, "github_wait");
    if !wait.is_empty() {
        o.push(
            updated,
            Actor::Github,
            None,
            "wait",
            line(wait),
            None,
            Tone::Neutral,
        );
    }

    let closed_at = s(item, "closed_at");
    if !closed_at.is_empty() {
        let by = item
            .get("closed_by_name")
            .or_else(|| item.get("closed_by"))
            .and_then(|v| v.as_s().ok())
            .cloned();
        let reason = s(item, "close_reason");
        o.push(
            closed_at,
            Actor::Person,
            by,
            "closed",
            if reason.is_empty() {
                "Closed the run".to_string()
            } else {
                line(&format!("Closed the run: {reason}"))
            },
            None,
            Tone::Neutral,
        );
    }

    let next = s(item, "continued_run_id");
    if !next.is_empty() && next != run_id {
        o.push(
            updated,
            Actor::Coderhelm,
            None,
            "continued",
            "Continued the work in a new run".to_string(),
            Some(format!("/runs/detail?id={}", urlencode(next))),
            Tone::Neutral,
        );
    }

    if closed_at.is_empty() {
        if let Some((text, tone)) = status_line(s(item, "status"), s(item, "error_message")) {
            let link = match s(item, "status") {
                "completed" | "merged" => pr_link.clone(),
                _ => None,
            };
            o.push(updated, Actor::Coderhelm, None, "status", text, link, tone);
        }
    }
    o.list
}

/// Where a run that isn't working right now stands, as an entry's words.
pub fn status_line(status: &str, error: &str) -> Option<(String, Tone)> {
    use common::run_status as st;
    let r = match status {
        st::COMPLETED | st::SUCCESS => ("Finished".to_string(), Tone::Good),
        st::MERGED => (
            "Finished; the pull request is merged".to_string(),
            Tone::Good,
        ),
        st::FAILED if !error.trim().is_empty() => {
            (line(&format!("Stopped: {error}")), Tone::Problem)
        }
        st::FAILED => ("Stopped with an error".to_string(), Tone::Problem),
        st::CANCELLED => ("Cancelled".to_string(), Tone::Neutral),
        st::NEEDS_INPUT => ("Waiting on a person".to_string(), Tone::Neutral),
        st::NEEDS_OUTSIDE_CHANGE => (
            "The fix is a change outside the code; handed to people".to_string(),
            Tone::Neutral,
        ),
        st::COVERED_BY_PR => (
            "An open pull request already covers this ticket".to_string(),
            Tone::Neutral,
        ),
        st::AWAITING_CI => (
            "Waiting for the pull request's checks".to_string(),
            Tone::Neutral,
        ),
        st::RETRIED => ("Retried in a new run".to_string(), Tone::Neutral),
        _ => return None,
    };
    Some(r)
}

trait LowerFirst {
    fn to_lowercase_first(&self) -> String;
}

impl LowerFirst for String {
    /// "Pull request" reads "pull request" mid-sentence; "CI fix" stays.
    fn to_lowercase_first(&self) -> String {
        let mut c = self.chars();
        match (c.next(), c.clone().next()) {
            (Some(f), Some(n)) if f.is_uppercase() && n.is_lowercase() => {
                f.to_lowercase().collect::<String>() + c.as_str()
            }
            _ => self.clone(),
        }
    }
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Each step's end, from its trace.
pub fn from_traces(items: &[Item], pr_url: &str) -> Vec<Entry> {
    let mut o = Out::new("trace");
    let mut sorted: Vec<&Item> = items.iter().collect();
    sorted.sort_by(|a, b| s(a, "timestamp").cmp(s(b, "timestamp")));
    for t in sorted {
        let pass = s(t, "pass");
        if pass.is_empty() {
            continue;
        }
        let label = step_label(pass);
        let took = t
            .get("duration_ms")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .map(|ms| format!(" in {}", duration(ms)))
            .unwrap_or_default();
        let error = s(t, "error");
        if !error.trim().is_empty() {
            o.push(
                s(t, "timestamp"),
                Actor::Coderhelm,
                None,
                "step_failed",
                line(&format!("{label} stopped: {error}")),
                None,
                Tone::Problem,
            );
        } else if pass == "pr" && !pr_url.is_empty() {
            o.push(
                s(t, "timestamp"),
                Actor::Coderhelm,
                None,
                "step_done",
                format!("Opened the pull request{took}"),
                Some(pr_url.to_string()),
                Tone::Good,
            );
        } else {
            o.push(
                s(t, "timestamp"),
                Actor::Coderhelm,
                None,
                "step_done",
                format!("{label} done{took}"),
                None,
                Tone::Neutral,
            );
        }
    }
    o.list
}

/// Approval cards: asked, and answered or expired.
pub fn from_approvals(list: &[Approval]) -> Vec<Entry> {
    let mut o = Out::new("approval");
    let mut sorted: Vec<&Approval> = list.iter().collect();
    sorted.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
    for a in sorted {
        let what = line(&a.headline());
        o.push(
            &a.created_at,
            Actor::Coderhelm,
            None,
            "approval",
            line(&format!("Asked for approval: {what}")),
            None,
            Tone::Neutral,
        );
        match a.status {
            ApprovalStatus::Approved | ApprovalStatus::Denied => {
                let (word, tone) = if a.status == ApprovalStatus::Approved {
                    ("Approved", Tone::Good)
                } else {
                    ("Denied", Tone::Problem)
                };
                o.push(
                    a.decided_at.as_deref().unwrap_or(&a.created_at),
                    Actor::Person,
                    Some(a.decided_by.clone()),
                    "approval_answered",
                    line(&format!("{word}: {what}")),
                    None,
                    tone,
                );
            }
            ApprovalStatus::Expired => o.push(
                a.decided_at.as_deref().unwrap_or(&a.expires_at),
                Actor::Coderhelm,
                None,
                "approval_answered",
                line(&format!("Nobody answered in time: {what}")),
                None,
                Tone::Problem,
            ),
            ApprovalStatus::Pending => {}
        }
    }
    o.list
}

/// Questions the run put to the team's own agents.
pub fn from_asks(list: &[AgentAsk]) -> Vec<Entry> {
    let mut o = Out::new("ask");
    let mut sorted: Vec<&AgentAsk> = list.iter().collect();
    sorted.sort_by(|a, b| a.at.cmp(&b.at).then(a.id.cmp(&b.id)));
    for a in sorted {
        let name = a
            .path
            .rsplit('/')
            .next()
            .unwrap_or(&a.path)
            .trim_end_matches(".md");
        let (how, tone) = match a.outcome.as_str() {
            "answered" if a.ms > 0 => (
                format!(", answered in {}", duration(a.ms as u64)),
                Tone::Neutral,
            ),
            "answered" => (", answered".to_string(), Tone::Neutral),
            "pending" => (", waiting for its answer".to_string(), Tone::Neutral),
            "timed_out" => (", no answer in time".to_string(), Tone::Problem),
            _ => (", it couldn't answer".to_string(), Tone::Problem),
        };
        o.push(
            &a.at,
            Actor::Coderhelm,
            None,
            "agent_ask",
            line(&format!("Asked the team agent {name}{how}")),
            Some(format!(
                "/skills/agent?repo={}&path={}",
                urlencode(&a.repo),
                urlencode(&a.path)
            )),
            tone,
        );
    }
    o.list
}

/// CoderHelm's review verdicts on one pull request, from its review records.
pub fn from_reviews(items: &[Item], pr: u64) -> Vec<Entry> {
    let mut o = Out::new("review");
    let mut sorted: Vec<&Item> = items.iter().collect();
    sorted.sort_by(|a, b| s(a, "created_at").cmp(s(b, "created_at")));
    for it in sorted {
        let (text, tone) = match s(it, "verdict") {
            "APPROVE" => ("approved", Tone::Good),
            "REQUEST_CHANGES" => ("asked for changes", Tone::Problem),
            _ => continue,
        };
        if s(it, "body")
            .trim_start()
            .starts_with(super::run_checks::LEGACY_FAILED_PREFIX)
        {
            continue;
        }
        o.push(
            s(it, "created_at"),
            Actor::Coderhelm,
            None,
            "review",
            format!("Coderhelm's review of #{pr} {text}"),
            Some(format!("/reviewer/detail?sk={}", urlencode(s(it, "sk")))),
            tone,
        );
    }
    o.list
}

/// Whether a delivery is about one of the run's pull requests.
fn about_run(d: &Delivery, prs: &[RunPr]) -> bool {
    d.pr.is_some_and(|n| {
        prs.iter()
            .any(|p| p.pr_number == n && p.repo.eq_ignore_ascii_case(&d.repo))
    })
}

/// Events and messages tied to the run's pull requests.
pub fn from_deliveries(list: &[Delivery], prs: &[RunPr]) -> Vec<Entry> {
    let mut o = Out::new("delivery");
    let mut sorted: Vec<&Delivery> = list.iter().filter(|d| about_run(d, prs)).collect();
    sorted.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
    sorted.truncate(MAX_DELIVERIES);
    for d in sorted {
        let actor = match (d.direction, d.source.as_str()) {
            (Direction::Inbound, "github" | "relay") => Actor::Github,
            (Direction::Inbound, "jira" | "relay_jira") => Actor::Jira,
            _ => Actor::Coderhelm,
        };
        let verb = match d.direction {
            Direction::Inbound => "Received",
            Direction::Outbound => "Sent",
        };
        let outcome = if d.outcome.is_empty() {
            String::new()
        } else {
            format!(": {}", d.outcome)
        };
        o.push(
            &d.created_at,
            actor,
            None,
            "delivery",
            line(&format!("{verb} {} {}{outcome}", d.source, d.event)),
            Some("/settings/deliveries".to_string()),
            if d.status.is_ok() {
                Tone::Neutral
            } else {
                Tone::Problem
            },
        );
    }
    o.list
}

/// All of a run's entries, oldest first, at most [`MAX_ENTRIES`] (the
/// newest kept).
pub fn merge(groups: Vec<Vec<Entry>>) -> Vec<Entry> {
    let mut all: Vec<Entry> = groups.into_iter().flatten().collect();
    all.sort_by(|a, b| a.id.cmp(&b.id));
    all.dedup_by(|a, b| a.id == b.id);
    if all.len() > MAX_ENTRIES {
        all.drain(..all.len() - MAX_ENTRIES);
    }
    all
}

/// One page from the newest end: the last `limit` entries before `before`
/// (all of them when None), oldest first, and the cursor for the page
/// before it.
pub fn page(all: &[Entry], before: Option<&str>, limit: usize) -> (Vec<Entry>, Option<String>) {
    let limit = limit.clamp(1, MAX_PAGE);
    let end = match before.filter(|b| !b.is_empty()) {
        Some(b) => all.partition_point(|e| e.id.as_str() < b),
        None => all.len(),
    };
    let start = end.saturating_sub(limit);
    let earlier = (start > 0).then(|| all[start].id.clone());
    (all[start..end].to_vec(), earlier)
}

fn fail(code: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({ "error": message })))
}

/// GET /api/runs/:run_id/activity?before=&limit=
pub async fn activity(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(run_id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Reply {
    let team = claims.team_id.clone();
    let got = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team.clone()))
        .key("run_id", AttributeValue::S(run_id.clone()))
        .send()
        .await
        .map_err(|e| {
            warn!(error = %e, "run activity: read run");
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Couldn't read the run. Try again.",
            )
        })?;
    let item = got
        .item()
        .cloned()
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No run with this id in the team."))?;

    let prs: Vec<RunPr> = common::run_prs::from_item(&item)
        .into_iter()
        .filter(|p| p.pr_number > 0)
        .take(MAX_PRS)
        .collect();
    let since = s(&item, "created_at").to_string();
    // Anyone who sees the run sees its story; the deliveries log (headers,
    // payloads) is for admins, as on its own page, so only they get those
    // entries. Not a guard: the route is for every viewer.
    let admin = claims.role_level() >= 3;

    let lookups = super::run_trigger::load_lookups(&state, &team, [&item]).await;
    let trigger = super::run_trigger::trigger(&item, &lookups);

    let traces = async {
        if state.config.traces_table_name.is_empty() {
            return Vec::new();
        }
        state
            .dynamo
            .query()
            .table_name(&state.config.traces_table_name)
            .key_condition_expression("team_id = :tid AND begins_with(sk, :prefix)")
            .expression_attribute_values(":tid", AttributeValue::S(team.clone()))
            .expression_attribute_values(
                ":prefix",
                AttributeValue::S(format!("RUN#{run_id}#PASS#")),
            )
            .limit(200)
            .send()
            .await
            .map(|r| r.items().to_vec())
            .unwrap_or_else(|e| {
                warn!(error = %e, "run activity: traces");
                Vec::new()
            })
    };
    let approvals = async {
        super::agent_policy::store(&state)
            .list_approvals(&team, false)
            .await
            .map(|l| {
                l.into_iter()
                    .filter(|a| a.run_id == run_id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|e| {
                warn!(error = %e, "run activity: approvals");
                Vec::new()
            })
    };
    let asks = async {
        common::skills::SkillStore {
            dynamo: &state.dynamo,
            table: &state.config.settings_table_name,
            team: &team,
        }
        .asks_for_run(&run_id)
        .await
        .unwrap_or_else(|e| {
            warn!(error = %e, "run activity: agent asks");
            Vec::new()
        })
    };
    let reviews = async {
        let mut out = Vec::new();
        for p in &prs {
            let items = state
                .dynamo
                .query()
                .table_name(&state.config.settings_table_name)
                .key_condition_expression("pk = :pk AND begins_with(sk, :sk)")
                .expression_attribute_values(":pk", AttributeValue::S(team.clone()))
                .expression_attribute_values(
                    ":sk",
                    AttributeValue::S(format!("REVIEW#{}#{:0>6}#", p.repo, p.pr_number)),
                )
                .projection_expression("sk, verdict, created_at, body")
                .scan_index_forward(false)
                .limit(REVIEWS_PER_PR)
                .send()
                .await
                .map(|r| r.items().to_vec())
                .unwrap_or_else(|e| {
                    warn!(error = %e, "run activity: reviews");
                    Vec::new()
                });
            out.push(from_reviews(&items, p.pr_number));
        }
        out.into_iter().flatten().collect::<Vec<_>>()
    };
    let deliveries = async {
        if !admin || prs.is_empty() || since.is_empty() {
            return Vec::new();
        }
        let f = common::deliveries::Filter {
            since: Some(since.clone()),
            ..Default::default()
        };
        common::deliveries::list(
            &state.dynamo,
            &state.config.settings_table_name,
            &team,
            &f,
            500,
        )
        .await
        .unwrap_or_else(|e| {
            warn!(error = %e, "run activity: deliveries");
            Vec::new()
        })
    };
    let (traces, approvals, asks, reviews, deliveries) =
        tokio::join!(traces, approvals, asks, reviews, deliveries);

    let all = merge(vec![
        from_run(&item, &trigger, &run_id),
        from_traces(&traces, s(&item, "pr_url")),
        from_approvals(&approvals),
        from_asks(&asks),
        reviews,
        from_deliveries(&deliveries, &prs),
    ]);
    let limit = q
        .get("limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(PAGE);
    let (entries, earlier) = page(&all, q.get("before").map(String::as_str), limit);
    Ok(Json(json!({
        "entries": entries,
        "earlier": earlier,
        "total": all.len(),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sv(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_string())
    }

    fn m(pairs: &[(&str, &str)]) -> AttributeValue {
        AttributeValue::M(pairs.iter().map(|(k, v)| (k.to_string(), sv(v))).collect())
    }

    fn trigger() -> RunTrigger {
        RunTrigger {
            kind: TriggerKind::Jira,
            label: "Jira".into(),
            r#ref: Some("WEB-42".into()),
            ref_url: Some("https://example.atlassian.net/browse/WEB-42".into()),
            by: None,
        }
    }

    fn run() -> Item {
        HashMap::from([
            ("run_id".to_string(), sv("r1")),
            ("status".to_string(), sv("completed")),
            ("created_at".to_string(), sv("2026-10-22T14:03:11Z")),
            ("updated_at".to_string(), sv("2026-10-22T14:40:00+00:00")),
            (
                "pr_url".to_string(),
                sv("https://github.com/acme/web/pull/7"),
            ),
            (
                "pass_history".to_string(),
                AttributeValue::L(vec![
                    m(&[("pass", "triage"), ("started_at", "2026-10-22T14:03:12Z")]),
                    m(&[
                        ("pass", "implement"),
                        ("started_at", "2026-10-22T14:05:40Z"),
                    ]),
                    m(&[("pass", "ci_fix:2"), ("started_at", "2026-10-22T14:30:00Z")]),
                ]),
            ),
            (
                "progress_notes".to_string(),
                AttributeValue::L(vec![m(&[
                    ("message", "Tests pass; opening the pull request.\nmore"),
                    ("timestamp", "2026-10-22T14:15:02.5+00:00"),
                ])]),
            ),
            (
                "ci_fixes_at".to_string(),
                AttributeValue::L(vec![sv("2026-10-22T14:35:00Z")]),
            ),
        ])
    }

    #[test]
    fn times_sort_as_instants_whatever_their_form() {
        assert_eq!(
            norm("2026-10-22T16:03:11+02:00").as_deref(),
            Some("2026-10-22T14:03:11.000Z")
        );
        assert_eq!(norm("not a time"), None);
        assert_eq!(norm(""), None);
    }

    #[test]
    fn the_run_record_tells_its_story_in_order() {
        let all = merge(vec![from_run(&run(), &trigger(), "r1")]);
        let texts: Vec<&str> = all.iter().map(|e| e.text.as_str()).collect();
        assert_eq!(
            texts,
            [
                "Run started from Jira WEB-42",
                "Started triage",
                "Started build",
                "Tests pass; opening the pull request.",
                "Started CI fix #2",
                "Pushed a fix for the failing checks",
                "Finished",
            ]
        );
        assert_eq!(all[0].actor, Actor::Jira);
        assert_eq!(
            all[0].link.as_deref(),
            Some("https://example.atlassian.net/browse/WEB-42")
        );
        assert_eq!(all.last().unwrap().tone, Tone::Good);
        assert_eq!(all.last().unwrap().at, "2026-10-22T14:40:00.000Z");
    }

    #[test]
    fn sources_merge_by_time_and_the_newest_comes_last() {
        let run_entries = from_run(&run(), &trigger(), "r1");
        let traces = vec![
            HashMap::from([
                ("pass".to_string(), sv("pr")),
                ("timestamp".to_string(), sv("2026-10-22T14:16:00Z")),
                ("duration_ms".to_string(), AttributeValue::N("65000".into())),
            ]),
            HashMap::from([
                ("pass".to_string(), sv("implement")),
                ("timestamp".to_string(), sv("2026-10-22T14:14:00Z")),
                ("error".to_string(), sv("Ran out of time")),
            ]),
        ];
        let reviews = vec![
            HashMap::from([
                ("sk".to_string(), sv("REVIEW#acme/web#000007#a")),
                ("verdict".to_string(), sv("REQUEST_CHANGES")),
                ("created_at".to_string(), sv("2026-10-22T14:20:00Z")),
            ]),
            HashMap::from([
                ("sk".to_string(), sv("REVIEW#acme/web#000007#q")),
                ("verdict".to_string(), sv("")),
                ("created_at".to_string(), sv("2026-10-22T14:21:00Z")),
            ]),
        ];
        let all = merge(vec![
            from_traces(&traces, "https://github.com/acme/web/pull/7"),
            from_reviews(&reviews, 7),
            run_entries,
        ]);
        let ids: Vec<&str> = all.iter().map(|e| e.id.as_str()).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "oldest first");
        let pos = |kind: &str| all.iter().position(|e| e.kind == kind).unwrap();
        assert!(pos("step_failed") < pos("step_done"));
        assert!(pos("step_done") < pos("review"));
        assert_eq!(
            all[pos("step_failed")].text,
            "Build stopped: Ran out of time"
        );
        assert_eq!(
            all[pos("step_done")].text,
            "Opened the pull request in 1m 5s"
        );
        assert_eq!(
            all.iter().filter(|e| e.kind == "review").count(),
            1,
            "answers between verdicts aren't verdicts"
        );
        assert_eq!(all.last().unwrap().kind, "status");
    }

    #[test]
    fn entries_at_the_same_time_stay_apart_and_merge_keeps_one_of_each() {
        let mut o = Out::new("x");
        for _ in 0..3 {
            o.push(
                "2026-10-22T14:00:00Z",
                Actor::Coderhelm,
                None,
                "note",
                "same".into(),
                None,
                Tone::Neutral,
            );
        }
        let dup = o.list.clone();
        let all = merge(vec![o.list, dup]);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn a_page_comes_from_the_newest_end_with_a_cursor_to_older() {
        let mut o = Out::new("x");
        for i in 0..5 {
            o.push(
                &format!("2026-10-22T14:0{i}:00Z"),
                Actor::Coderhelm,
                None,
                "note",
                format!("n{i}"),
                None,
                Tone::Neutral,
            );
        }
        let all = merge(vec![o.list]);
        let (p1, earlier) = page(&all, None, 2);
        assert_eq!(
            p1.iter().map(|e| e.text.as_str()).collect::<Vec<_>>(),
            ["n3", "n4"]
        );
        let (p2, earlier) = page(&all, earlier.as_deref(), 2);
        assert_eq!(
            p2.iter().map(|e| e.text.as_str()).collect::<Vec<_>>(),
            ["n1", "n2"]
        );
        let (p3, earlier) = page(&all, earlier.as_deref(), 2);
        assert_eq!(
            p3.iter().map(|e| e.text.as_str()).collect::<Vec<_>>(),
            ["n0"]
        );
        assert_eq!(earlier, None);
        assert_eq!(page(&all, None, 0).0.len(), 1, "at least one");
    }

    #[test]
    fn merge_keeps_the_newest_when_there_are_too_many() {
        let mut o = Out::new("x");
        for i in 0..(MAX_ENTRIES + 5) {
            let at = chrono::DateTime::parse_from_rfc3339("2026-10-22T00:00:00Z").unwrap()
                + chrono::Duration::seconds(i as i64);
            o.push(
                &at.to_rfc3339(),
                Actor::Coderhelm,
                None,
                "note",
                format!("n{i}"),
                None,
                Tone::Neutral,
            );
        }
        let all = merge(vec![o.list]);
        assert_eq!(all.len(), MAX_ENTRIES);
        assert_eq!(all.last().unwrap().text, format!("n{}", MAX_ENTRIES + 4));
        assert_eq!(all[0].text, "n5");
    }

    #[test]
    fn answers_and_questions_name_who_and_link_the_comment() {
        let mut item = run();
        item.insert(
            common::run_questions::FIELD_QUESTIONS.to_string(),
            sv(r#"{"questions":[{"id":"q1","question":"Which environments?"},{"id":"q2","question":"What limit?"}],"asked_at":"2026-10-22T14:10:00Z","comment_url":"https://example.atlassian.net/c/1"}"#),
        );
        item.insert(
            common::run_questions::FIELD_ANSWERS.to_string(),
            sv(r#"[{"question_id":"q1","question":"Which environments?","answer":"Production only.","author":"Pat Doe","answered_at":"2026-10-22T14:12:00Z"}]"#),
        );
        item.insert(
            "answers_url".to_string(),
            sv("https://example.atlassian.net/c/2"),
        );
        let all = from_run(&item, &trigger(), "r1");
        let asked = all.iter().find(|e| e.kind == "asked").unwrap();
        assert_eq!(asked.text, "Asked 2 questions on the ticket");
        assert_eq!(
            asked.link.as_deref(),
            Some("https://example.atlassian.net/c/1")
        );
        let answered = all.iter().find(|e| e.kind == "answered").unwrap();
        assert_eq!(answered.actor, Actor::Person);
        assert_eq!(answered.who.as_deref(), Some("Pat Doe"));
        assert_eq!(
            answered.text,
            "Answered \"Which environments?\": Production only."
        );
    }

    #[test]
    fn a_closed_run_says_who_closed_it_instead_of_its_status() {
        let mut item = run();
        item.insert("status".to_string(), sv("closed"));
        item.insert("closed_at".to_string(), sv("2026-10-22T15:00:00Z"));
        item.insert("closed_by_name".to_string(), sv("Pat Doe"));
        item.insert("close_reason".to_string(), sv("Duplicate"));
        let all = from_run(&item, &trigger(), "r1");
        assert!(all.iter().all(|e| e.kind != "status"));
        let c = all.iter().find(|e| e.kind == "closed").unwrap();
        assert_eq!(
            (c.who.as_deref(), c.text.as_str()),
            (Some("Pat Doe"), "Closed the run: Duplicate")
        );
    }

    #[test]
    fn a_running_run_has_no_status_line_and_a_wait_shows() {
        let mut item = run();
        item.insert("status".to_string(), sv("running"));
        item.insert(
            "github_wait".to_string(),
            sv("Waiting for GitHub's hourly limit; continues by itself at 21:05 UTC"),
        );
        let all = from_run(&item, &trigger(), "r1");
        assert!(all.iter().all(|e| e.kind != "status"));
        assert_eq!(all.iter().filter(|e| e.kind == "wait").count(), 1);
    }

    #[test]
    fn deliveries_count_only_for_the_runs_pull_requests() {
        let prs = vec![RunPr {
            repo: "acme/web".into(),
            pr_number: 7,
            ..Default::default()
        }];
        let mut ours = Delivery::new(
            Direction::Inbound,
            "github",
            "pull_request.synchronize",
            "t",
        );
        ours.repo = "Acme/Web".into();
        ours.pr = Some(7);
        ours.created_at = "2026-10-22T14:30:00Z".into();
        ours.outcome = "Queued a review".into();
        let mut other = ours.clone();
        other.pr = Some(8);
        let all = from_deliveries(&[ours, other], &prs);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].actor, Actor::Github);
        assert_eq!(
            all[0].text,
            "Received github pull_request.synchronize: Queued a review"
        );
    }

    #[test]
    fn steps_read_as_people_name_them() {
        assert_eq!(step_label("implement"), "Build");
        assert_eq!(step_label("ci_fix:2"), "CI fix #2");
        assert_eq!(step_label("plan:1"), "Plan");
        assert_eq!(step_label("outside_change"), "Outside change");
        assert_eq!(
            "Pull request".to_string().to_lowercase_first(),
            "pull request"
        );
        assert_eq!("CI fix #2".to_string().to_lowercase_first(), "CI fix #2");
    }

    #[test]
    fn long_lines_are_clipped_to_one() {
        let long = "x".repeat(400);
        assert_eq!(line(&long).chars().count(), MAX_TEXT);
        assert_eq!(line("one\ntwo"), "one");
    }
}
