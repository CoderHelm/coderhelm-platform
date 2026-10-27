//! A ticket's history with CoderHelm, read before a run plans code.
//!
//! A ticket runs again after a PR of CoderHelm's was closed without merging,
//! after a lesson said the fix belongs in another system, or after a person
//! wrote on the ticket that it was fixed elsewhere. The planner is shown that
//! history explicitly and must answer it, and [`TicketHistory::guard`] holds
//! the run to it in code: by default no second pull request is opened, and
//! never one for a ticket whose text hasn't changed since CoderHelm learned
//! the fix is outside the code.

use crate::passes::attr_s;
use crate::passes::fix_location::{self, FixDecision, FixLocation, OutsideChange, Route};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use common::ticket_state::{self, Intent, Note};
use std::collections::HashMap;
use tracing::warn;

/// One earlier run on the ticket, as its record shows it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PriorRun {
    pub run_id: String,
    pub status: String,
    pub repo: String,
    pub pr_number: u64,
    pub pr_url: String,
    /// "closed" (without merging), "merged", or empty (open or none).
    pub pr_outcome: String,
    pub cancel_reason: String,
    /// The lesson recorded on where the fix belongs, if any.
    pub lesson: String,
    /// The system the lesson names.
    pub lesson_where: String,
    pub fix_location: String,
    pub fix_decision: Option<FixDecision>,
    /// The ticket's content hash when the run started.
    pub context_hash: String,
    /// What people wrote on its PR, as (author, text), newest last.
    pub pr_comments: Vec<(String, String)>,
    /// Answers people gave in CoderHelm to the questions it stopped to ask.
    pub answers: Vec<common::run_questions::Answer>,
}

impl PriorRun {
    /// CoderHelm's pull request for this run was closed without merging.
    pub fn pr_closed_unmerged(&self) -> bool {
        self.pr_number > 0
            && (self.pr_outcome == "closed" || self.cancel_reason == "pr_closed_by_human")
            && self.pr_outcome != "merged"
    }

    /// The run concluded the fix is outside the code: a lesson was stored,
    /// the planner decided it, or the run ended asking for an outside change.
    pub fn concluded_outside(&self) -> bool {
        !self.lesson.trim().is_empty()
            || self.status == fix_location::STATUS_OUTSIDE
            || self.fix_location == FixLocation::OutsideCode.as_str()
    }
}

/// What CoderHelm knows about the ticket before this run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TicketHistory {
    /// Earlier runs, oldest first (the current run excluded).
    pub runs: Vec<PriorRun>,
    /// What people wrote on the ticket, as CoderHelm read it.
    pub notes: Vec<Note>,
    /// The ticket's hash when a person's comment marked it done (empty
    /// when it isn't).
    pub done_hash: String,
    /// Every pull request for this ticket in the team's repositories
    /// (CoderHelm's or anyone's), as GitHub shows it now, oldest first.
    pub prs: Vec<EarlierPr>,
    /// Choices people made on CoderHelm's notices (see
    /// `ticket_state::Decision`), oldest first.
    pub decisions: Vec<ticket_state::Decision>,
}

/// An earlier pull request for the ticket, read from GitHub: what it said,
/// what reviewers wrote, and how it ended (who closed or merged it, when).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EarlierPr {
    /// `owner/name`.
    pub repo: String,
    pub number: u64,
    pub title: String,
    pub author: String,
    pub branch: String,
    /// `open`, `merged` or `closed` (without merging).
    pub state: String,
    pub created_at: String,
    /// The description, as written.
    pub body: String,
    /// Conversation comments as (author, text), oldest first.
    pub comments: Vec<(String, String)>,
    /// Review summaries as (author, review state, text), oldest first.
    pub reviews: Vec<(String, String, String)>,
    /// Inline review comments as (author, path, text), oldest first.
    pub threads: Vec<(String, String, String)>,
    /// Who closed it and when, from the issue's `closed` event (for a
    /// merged pull request: who merged it).
    pub closed_by: String,
    pub closed_at: String,
}

impl EarlierPr {
    pub fn closed_unmerged(&self) -> bool {
        self.state == "closed"
    }

    /// The close date as `YYYY-MM-DD`, as plans are told to write it.
    pub fn closed_on(&self) -> &str {
        self.closed_at.get(..10).unwrap_or(&self.closed_at)
    }

    /// Whether people said anything on it (a reason may be there).
    pub fn has_discussion(&self) -> bool {
        !self.comments.is_empty() || !self.threads.is_empty() || !self.reviews.is_empty()
    }
}

/// Whether `text` names pull request `number` (`#123`, or a link ending
/// `/pull/123`), not a longer number that starts with it.
pub fn names_pr(text: &str, number: u64) -> bool {
    if number == 0 {
        return false;
    }
    let n = number.to_string();
    for mark in ["#", "/pull/", "/pulls/"] {
        let needle = format!("{mark}{n}");
        let mut from = 0;
        while let Some(i) = text[from..].find(&needle) {
            let end = from + i + needle.len();
            if !text[end..].starts_with(|c: char| c.is_ascii_digit()) {
                return true;
            }
            from = end;
        }
    }
    false
}

/// Where the fix belongs, as the history shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutsideConclusion {
    /// The ticket's hash when it was reached (empty when unknown).
    pub context_hash: String,
    /// The decision to report: the planner's own when it named a usable
    /// change, otherwise one built from the lesson.
    pub decision: FixDecision,
}

/// What the history allows this run to do with its plan.
#[derive(Debug, Clone, PartialEq)]
pub enum Guard {
    Proceed,
    /// No code: report `decision` on the ticket by `route` (OutsideOnly or Ask).
    Stop {
        route: Route,
        decision: Box<FixDecision>,
        why: String,
    },
}

impl TicketHistory {
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty() && self.notes.is_empty() && self.prs.is_empty()
    }

    /// Every earlier pull request for the ticket as `(repo, number)`: the
    /// ones read from GitHub, and any a run record names that wasn't.
    pub fn earlier_pr_numbers(&self) -> Vec<(String, u64)> {
        let mut out: Vec<(String, u64)> = self
            .prs
            .iter()
            .map(|p| (p.repo.clone(), p.number))
            .collect();
        for r in &self.runs {
            if r.pr_number > 0
                && !out
                    .iter()
                    .any(|(repo, n)| *n == r.pr_number && repo == &r.repo)
            {
                out.push((r.repo.clone(), r.pr_number));
            }
        }
        out
    }

    /// What a plan leaves out of the ticket's own history, checked on its
    /// text (`plan_text`: the openspec files and the decision's words):
    /// - the ticket has earlier pull requests and the plan names none;
    /// - a pull request closed without merging whose close event was read,
    ///   and the plan doesn't say who closed it and when.
    pub fn plan_gaps(&self, plan_text: &str) -> Vec<String> {
        let prs = self.earlier_pr_numbers();
        if prs.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::new();
        if !prs.iter().any(|(_, n)| names_pr(plan_text, *n)) {
            let list: Vec<String> = prs.iter().map(|(r, n)| format!("#{n} in {r}")).collect();
            out.push(format!(
                "This ticket has earlier pull requests ({}) and the plan names none of them. Read \
                 each (its description, review threads, how it ended), reuse what was right, and \
                 say in `history_check` what changes this time and why, by number.",
                list.join(", ")
            ));
        }
        let lower = plan_text.to_ascii_lowercase();
        for p in self
            .prs
            .iter()
            .filter(|p| p.closed_unmerged() && !p.closed_by.is_empty())
        {
            let who = lower.contains(&p.closed_by.to_ascii_lowercase());
            let when = p.closed_on().is_empty() || plan_text.contains(p.closed_on());
            if !(who && when) {
                out.push(format!(
                    "#{} in {} was closed without merging by {} on {} (its close event). Say who \
                     closed it and when in `history_check`{}, instead of saying it is unknown.",
                    p.number,
                    p.repo,
                    p.closed_by,
                    p.closed_on(),
                    if p.has_discussion() {
                        " and build on what its comments say"
                    } else {
                        ", and ask ONE question about why in `questions`"
                    }
                ));
            }
        }
        out
    }

    pub fn closed_unmerged(&self) -> Vec<&PriorRun> {
        self.runs
            .iter()
            .filter(|r| r.pr_closed_unmerged())
            .collect()
    }

    /// The newest conclusion that the fix is outside the code: from a run,
    /// or from a person's note on the ticket.
    pub fn outside_conclusion(&self) -> Option<OutsideConclusion> {
        let from_run = self.runs.iter().rev().find(|r| r.concluded_outside());
        let from_note = self
            .notes
            .iter()
            .rev()
            .find(|n| n.intent == Intent::FixElsewhereInfo);
        if from_run.is_none() && from_note.is_none() {
            return None;
        }
        let mut decision = from_run
            .and_then(|r| r.fix_decision.clone())
            .filter(|d| d.effective_location() == FixLocation::OutsideCode)
            .unwrap_or_default();
        if decision.effective_location() != FixLocation::OutsideCode {
            let lesson = from_run.map(|r| r.lesson.trim()).unwrap_or("");
            let lesson_where = from_run.map(|r| r.lesson_where.trim()).unwrap_or("");
            let said = from_note.map(|n| n.text.trim()).unwrap_or("");
            let cause = match (lesson.is_empty(), said.is_empty()) {
                (false, _) => lesson.to_string(),
                (true, false) => {
                    format!("A person wrote on the ticket: \"{}\"", one_line(said, 400))
                }
                (true, true) => "An earlier run found the fix is outside the code.".to_string(),
            };
            decision = FixDecision {
                fix_location: FixLocation::OutsideCode,
                cause,
                outside_change: (!lesson.is_empty() && !lesson_where.is_empty()).then(|| {
                    OutsideChange {
                        system: lesson_where.to_string(),
                        change: lesson.to_string(),
                        ..Default::default()
                    }
                }),
                ..Default::default()
            };
        }
        let context_hash = match from_run {
            Some(r) if !r.context_hash.is_empty() => r.context_hash.clone(),
            _ => self.done_hash.clone(),
        };
        Some(OutsideConclusion {
            context_hash,
            decision,
        })
    }

    /// The section the planner reads. Empty when the ticket has no history.
    pub fn planner_note(&self) -> String {
        if self.is_empty() {
            return String::new();
        }
        let mut out = String::from(
            "\n\n## This ticket's history (read before planning)\n\
             This ticket has been worked on before. Answer this history in `history_check` \
             before you plan any code, naming each earlier pull request by number: what you \
             reuse from it, what changes this time and why. If the history says the fix is \
             outside the code, or a person said it was fixed elsewhere, the default is NOT to \
             open another pull request: decide `outside_code` (or `unknown`) and reply on the \
             ticket instead.\n",
        );
        if !self.prs.is_empty() {
            out.push_str(
                "Earlier pull requests for this ticket, as GitHub shows them now (read each; a \
                 closed one's close event says who closed it and when):\n",
            );
            for p in &self.prs {
                out.push_str(&pr_note(p));
            }
        }
        for r in &self.runs {
            let pr = if r.pr_number > 0 {
                let ended = if r.pr_closed_unmerged() {
                    "closed WITHOUT merging"
                } else if r.pr_outcome == "merged" {
                    "merged"
                } else {
                    "open"
                };
                format!("pull request #{} ({}) {ended}", r.pr_number, r.repo)
            } else {
                "no pull request".to_string()
            };
            out.push_str(&format!("- Earlier run: {pr}; run ended {}.", r.status));
            if !r.fix_location.is_empty() {
                out.push_str(&format!(" Planner decided: {}.", r.fix_location));
            }
            if let Some(d) = &r.fix_decision {
                if !d.cause.trim().is_empty() {
                    out.push_str(&format!(" Cause found: {}", one_line(&d.cause, 400)));
                }
            }
            if !r.lesson.trim().is_empty() {
                out.push_str(&format!(
                    "\n  Lesson the team recorded from it: {}",
                    one_line(&r.lesson, 600)
                ));
            }
            for (author, text) in &r.pr_comments {
                out.push_str(&format!(
                    "\n  {author} wrote on that PR: {}",
                    one_line(text, 600)
                ));
            }
            out.push('\n');
        }
        if !self.notes.is_empty() {
            out.push_str(
                "What people wrote on the ticket, oldest first. A later comment corrects an \
                 earlier one. Where a person already traced the cause, start from their \
                 finding: check it against the data and the code, and build on it rather \
                 than starting over:\n",
            );
            let texts = fit_notes(
                self.notes
                    .iter()
                    .map(|n| common::jira_text::text_of(&n.text).trim().to_string())
                    .collect(),
                NOTES_BUDGET,
            );
            for (n, text) in self.notes.iter().zip(texts) {
                // Kept whole, line breaks included: a written investigation
                // (a query, a table of rows) reads as written.
                out.push_str(&format!(
                    "- {} {} ({}):\n  {}\n",
                    n.at.get(..16).unwrap_or(&n.at),
                    n.author,
                    n.intent.as_str(),
                    text.replace('\n', "\n  ")
                ));
            }
        }
        out
    }

    /// A stable digest of everything here that can change a plan.
    pub fn digest(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        for r in &self.runs {
            parts.push(format!(
                "{}|{}|{}|{}|{}|{}|{}",
                r.run_id,
                r.status,
                r.pr_number,
                r.pr_outcome,
                r.cancel_reason,
                r.lesson,
                r.fix_location
            ));
            for (a, t) in &r.pr_comments {
                parts.push(format!("{a}|{t}"));
            }
        }
        for n in &self.notes {
            parts.push(format!("{}|{}|{}", n.at, n.intent.as_str(), n.text));
        }
        for p in &self.prs {
            parts.push(format!(
                "pr|{}|{}|{}|{}|{}|{}|{}",
                p.repo, p.number, p.state, p.title, p.body, p.closed_by, p.closed_at
            ));
            for (a, t) in &p.comments {
                parts.push(format!("{a}|{t}"));
            }
            for (a, s, t) in p.reviews.iter().chain(&p.threads) {
                parts.push(format!("{a}|{s}|{t}"));
            }
        }
        let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
        common::sha256_hex(&refs)
    }

    /// What this run may do with its plan. A plan that already codes
    /// nothing proceeds (it reports by its own route). A code plan stops when:
    /// - the history concluded the fix is outside the code and the ticket's
    ///   text hasn't changed since, or the planner didn't answer the history;
    /// - an earlier CoderHelm PR was closed without merging and the planner
    ///   didn't say why a code change is right anyway.
    pub fn guard(&self, decision: Option<&FixDecision>, current_hash: &str) -> Guard {
        let route = fix_location::route(decision);
        if !matches!(route, Route::Code | Route::CodeAndOutside) {
            return Guard::Proceed;
        }
        let answered = decision.is_some_and(|d| !d.history_check.trim().is_empty());
        // A plan traced to the code that writes the wrong value (code it
        // opened in this run, checked before here), with the defect measured
        // and the history answered, is newer evidence than an earlier "outside
        // the code" conclusion: that conclusion may be the very mistake the
        // trace corrects, and an unchanged ticket text doesn't make it right.
        let traced = answered
            && decision.is_some_and(|d| {
                d.evidence.producer.kind.trim() == "code"
                    && !d.evidence.producer.where_.trim().is_empty()
                    && !d.evidence.scope.trim().is_empty()
            });
        if traced {
            return Guard::Proceed;
        }
        let closed = self.closed_unmerged();
        if let Some(c) = self.outside_conclusion() {
            let unchanged = c.context_hash.is_empty() || c.context_hash == current_hash;
            if unchanged || !answered {
                let mut d = c.decision;
                let prs: Vec<String> = closed.iter().map(|r| format!("#{}", r.pr_number)).collect();
                let lead = if prs.is_empty() {
                    "CoderHelm already found that the fix for this ticket is outside the code."
                        .to_string()
                } else {
                    format!(
                        "CoderHelm's earlier pull request for this ticket ({}) was closed without \
                         merging, and the fix was found to be outside the code.",
                        prs.join(", ")
                    )
                };
                d.cause = format!("{lead} {}", d.cause.trim());
                let route = match d.effective_location() {
                    FixLocation::OutsideCode => Route::OutsideOnly,
                    _ => {
                        d.fix_location = FixLocation::Unknown;
                        Route::Ask
                    }
                };
                let why = if unchanged {
                    "the ticket is unchanged since CoderHelm learned the fix is outside the code"
                } else {
                    "the plan doesn't answer the ticket's history"
                };
                return Guard::Stop {
                    route,
                    decision: Box::new(d),
                    why: why.to_string(),
                };
            }
            return Guard::Proceed;
        }
        if !closed.is_empty() && !answered {
            let prs: Vec<String> = closed.iter().map(|r| format!("#{}", r.pr_number)).collect();
            return Guard::Stop {
                route: Route::Ask,
                decision: Box::new(FixDecision {
                    fix_location: FixLocation::Unknown,
                    cause: format!(
                        "CoderHelm's earlier pull request for this ticket ({}) was closed without \
                         merging, and this plan doesn't say what is different now, so I didn't \
                         open another one.",
                        prs.join(", ")
                    ),
                    ..Default::default()
                }),
                why: "an earlier PR was closed unmerged and the plan doesn't answer it".to_string(),
            };
        }
        Guard::Proceed
    }
}

/// Characters of ticket comments the planner reads in all. Each comment is
/// given whole; only a ticket past this total has comments shortened.
const NOTES_BUDGET: usize = 60_000;
/// What an oldest comment keeps when the total has to be cut.
const NOTE_FLOOR: usize = 300;

/// The comments' texts within `budget` characters. When they don't fit, the
/// oldest are shortened first (a later comment corrects an earlier one), each
/// down to `NOTE_FLOOR`, with a mark saying how much was left out.
fn fit_notes(mut texts: Vec<String>, budget: usize) -> Vec<String> {
    let total: usize = texts.iter().map(|t| t.chars().count()).sum();
    let mut excess = total.saturating_sub(budget);
    for t in texts.iter_mut() {
        if excess == 0 {
            break;
        }
        let len = t.chars().count();
        if len <= NOTE_FLOOR {
            continue;
        }
        let cut = excess.min(len - NOTE_FLOOR);
        let keep: String = t.chars().take(len - cut).collect();
        *t = format!("{keep} … ({cut} characters of this older comment left out)");
        excess -= cut;
    }
    texts
}

fn one_line(s: &str, max: usize) -> String {
    let joined = s.split_whitespace().collect::<Vec<_>>().join(" ");
    common::truncate_str(&joined, max).to_string()
}

type Item = HashMap<String, AttributeValue>;

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn prior_run(item: &Item) -> PriorRun {
    PriorRun {
        run_id: s(item, "run_id"),
        status: s(item, "status"),
        repo: s(item, "repo"),
        pr_number: item
            .get("pr_number")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0),
        pr_url: s(item, "pr_url"),
        pr_outcome: s(item, "pr_outcome"),
        cancel_reason: s(item, "cancel_reason"),
        lesson: s(item, "lesson"),
        lesson_where: s(item, "lesson_where"),
        fix_location: s(item, "fix_location"),
        fix_decision: serde_json::from_str(&s(item, "fix_decision")).ok(),
        context_hash: s(item, "context_hash"),
        pr_comments: Vec::new(),
        answers: serde_json::from_str(&s(item, common::run_questions::FIELD_ANSWERS))
            .unwrap_or_default(),
    }
}

/// Adds what people wrote on the earlier PRs that matter (closed unmerged,
/// or where the fix was found outside the code). Best-effort.
pub async fn add_pr_comments(
    history: &mut TicketHistory,
    github: &crate::clients::github::GitHubClient,
) {
    for r in history
        .runs
        .iter_mut()
        .filter(|r| r.pr_number > 0 && (r.pr_closed_unmerged() || r.concluded_outside()))
    {
        let Some((owner, name)) = r.repo.split_once('/') else {
            continue;
        };
        match github.list_issue_comments(owner, name, r.pr_number).await {
            Ok(c) => r.pr_comments = super::review_pr::human_comments(&c, 6),
            Err(e) => {
                warn!(pr = r.pr_number, error = %e, "Could not read an earlier PR's comments")
            }
        }
    }
}

/// The most pull requests read for one ticket, and repositories searched.
const MAX_PRS: usize = 8;
const MAX_REPOS: usize = 10;

/// How one earlier pull request reads in the planner's history.
fn pr_note(p: &EarlierPr) -> String {
    let ended = match p.state.as_str() {
        "merged" => match (p.closed_by.is_empty(), p.closed_on()) {
            (false, d) => format!("merged by {} on {d}", p.closed_by),
            (true, _) => "merged".to_string(),
        },
        "closed" => {
            let by = if p.closed_by.is_empty() {
                "closed WITHOUT merging (the close event couldn't be read)".to_string()
            } else {
                format!(
                    "closed WITHOUT merging by {} on {} (close event)",
                    p.closed_by,
                    p.closed_on()
                )
            };
            if p.has_discussion() {
                by
            } else {
                format!("{by}; nobody wrote a reason on it")
            }
        }
        _ => "still open".to_string(),
    };
    let mut out = format!(
        "- #{} in {} \"{}\" by {}{}, opened {}: {ended}.\n",
        p.number,
        p.repo,
        one_line(&p.title, 200),
        p.author,
        if p.branch.is_empty() {
            String::new()
        } else {
            format!(" (branch `{}`)", p.branch)
        },
        p.created_at.get(..10).unwrap_or(&p.created_at),
    );
    if !p.body.trim().is_empty() {
        out.push_str(&format!("  Description: {}\n", one_line(&p.body, 2500)));
    }
    for (author, state, text) in &p.reviews {
        out.push_str(&format!(
            "  Review by {author} ({state}): {}\n",
            one_line(text, 800)
        ));
    }
    for (author, path, text) in &p.threads {
        out.push_str(&format!(
            "  {author} on `{path}`: {}\n",
            one_line(text, 600)
        ));
    }
    for (author, text) in &p.comments {
        out.push_str(&format!("  {author} wrote: {}\n", one_line(text, 800)));
    }
    out
}

/// Adds every pull request for the ticket in `repos` (`owner/name`, the
/// team's repositories, searched in order): the ones the ticket's runs
/// opened, and any whose title, branch or description names the ticket.
/// Each is read in full: description, comments, reviews, inline review
/// threads, and how it ended (GitHub's `closed` event: who and when; for a
/// merged one, who merged it). Best-effort, bounded by [`MAX_PRS`].
pub async fn add_earlier_prs(
    history: &mut TicketHistory,
    github: &crate::clients::github::GitHubClient,
    repos: &[String],
    ticket_id: &str,
) {
    let mut found: Vec<(String, u64)> = Vec::new();
    for r in &history.runs {
        if r.pr_number > 0
            && r.repo.contains('/')
            && !found.contains(&(r.repo.clone(), r.pr_number))
        {
            found.push((r.repo.clone(), r.pr_number));
        }
    }
    if !ticket_id.is_empty() {
        for repo in repos.iter().take(MAX_REPOS) {
            let Some((owner, name)) = repo.split_once('/') else {
                continue;
            };
            match github.list_pull_requests(owner, name, "all", 100).await {
                Ok(v) => {
                    for p in v.as_array().into_iter().flatten() {
                        let number = p["number"].as_u64().unwrap_or(0);
                        let names = [&p["title"], &p["head"]["ref"], &p["body"]]
                            .iter()
                            .any(|t| super::mentions_ticket(t.as_str().unwrap_or(""), ticket_id));
                        let key = (repo.clone(), number);
                        if number > 0 && names && !found.contains(&key) {
                            found.push(key);
                        }
                    }
                }
                Err(e) => {
                    warn!(repo, error = %e, "Could not list pull requests for the ticket's history")
                }
            }
        }
    }
    found.truncate(MAX_PRS);
    for (repo, number) in found {
        if let Some(p) = read_pr(github, &repo, number).await {
            history.prs.push(p);
        }
    }
    history.prs.sort_by(|a, b| a.created_at.cmp(&b.created_at));
}

/// One pull request read in full. None when it can't be read.
async fn read_pr(
    github: &crate::clients::github::GitHubClient,
    repo: &str,
    number: u64,
) -> Option<EarlierPr> {
    let (owner, name) = repo.split_once('/')?;
    let pr = match github.get_pull_request(owner, name, number).await {
        Ok(v) => v,
        Err(e) => {
            warn!(repo, pr = number, error = %e, "Could not read an earlier pull request");
            return None;
        }
    };
    let text = |v: &serde_json::Value| v.as_str().unwrap_or("").to_string();
    let merged = pr["merged"].as_bool().unwrap_or(false) || pr["merged_at"].is_string();
    let state = if merged {
        "merged"
    } else if pr["state"].as_str() == Some("closed") {
        "closed"
    } else {
        "open"
    };
    let mut p = EarlierPr {
        repo: repo.to_string(),
        number,
        title: text(&pr["title"]),
        author: text(&pr["user"]["login"]),
        branch: text(&pr["head"]["ref"]),
        state: state.to_string(),
        created_at: text(&pr["created_at"]),
        body: common::truncate_str(pr["body"].as_str().unwrap_or(""), 6000).to_string(),
        ..Default::default()
    };
    if merged {
        p.closed_by = text(&pr["merged_by"]["login"]);
        p.closed_at = text(&pr["merged_at"]);
    } else if state == "closed" {
        match github.list_issue_events(owner, name, number).await {
            Ok(events) => {
                if let Some((by, at)) = close_event(&events) {
                    p.closed_by = by;
                    p.closed_at = at;
                }
            }
            Err(e) => {
                warn!(repo, pr = number, error = %e, "Could not read a pull request's events")
            }
        }
        if p.closed_at.is_empty() {
            p.closed_at = text(&pr["closed_at"]);
        }
    }
    match github.list_issue_comments(owner, name, number).await {
        Ok(c) => p.comments = others_comments(&c, 8),
        Err(e) => warn!(repo, pr = number, error = %e, "Could not read a pull request's comments"),
    }
    match github.list_pr_reviews(owner, name, number).await {
        Ok(v) => p.reviews = reviews_of(&v, 6),
        Err(e) => warn!(repo, pr = number, error = %e, "Could not read a pull request's reviews"),
    }
    match github.get_review_comments(owner, name, number).await {
        Ok(c) => p.threads = threads_of(&c, 10),
        Err(e) => {
            warn!(repo, pr = number, error = %e, "Could not read a pull request's review threads")
        }
    }
    Some(p)
}

/// Who closed an issue or pull request last, and when, from its events.
fn close_event(events: &serde_json::Value) -> Option<(String, String)> {
    events.as_array()?.iter().rev().find_map(|e| {
        (e["event"].as_str() == Some("closed")).then(|| {
            (
                e["actor"]["login"].as_str().unwrap_or("").to_string(),
                e["created_at"].as_str().unwrap_or("").to_string(),
            )
        })
    })
}

/// The last `max` conversation comments not written by CoderHelm itself
/// (its own progress notes say nothing about the work), as (author, text).
fn others_comments(comments: &[serde_json::Value], max: usize) -> Vec<(String, String)> {
    let all: Vec<(String, String)> = comments
        .iter()
        .filter_map(|c| {
            let author = c["user"]["login"].as_str()?;
            let body = c["body"].as_str()?.trim();
            (!super::is_own_login(author) && !body.is_empty())
                .then(|| (author.to_string(), body.to_string()))
        })
        .collect();
    let skip = all.len().saturating_sub(max);
    all.into_iter().skip(skip).collect()
}

/// The last `max` reviews that say something (a text, or a verdict), as
/// (author, state, text).
fn reviews_of(reviews: &serde_json::Value, max: usize) -> Vec<(String, String, String)> {
    let all: Vec<(String, String, String)> = reviews
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| {
            let author = r["user"]["login"].as_str()?.to_string();
            let state = r["state"].as_str().unwrap_or("").to_string();
            let body = r["body"].as_str().unwrap_or("").trim().to_string();
            let verdict = matches!(state.as_str(), "APPROVED" | "CHANGES_REQUESTED");
            (!body.is_empty() || verdict).then_some((author, state, body))
        })
        .collect();
    let skip = all.len().saturating_sub(max);
    all.into_iter().skip(skip).collect()
}

/// The last `max` inline review comments, as (author, path, text).
fn threads_of(comments: &[serde_json::Value], max: usize) -> Vec<(String, String, String)> {
    let all: Vec<(String, String, String)> = comments
        .iter()
        .filter_map(|c| {
            let author = c["user"]["login"].as_str()?.to_string();
            let body = c["body"].as_str()?.trim().to_string();
            (!body.is_empty()).then(|| (author, c["path"].as_str().unwrap_or("").to_string(), body))
        })
        .collect();
    let skip = all.len().saturating_sub(max);
    all.into_iter().skip(skip).collect()
}

/// Reads the ticket's earlier runs and its notes. Best-effort: a read error
/// gives what was read (an empty history at worst), logged.
pub async fn load(
    state: &WorkerState,
    team_id: &str,
    ticket_id: &str,
    current_run_id: &str,
) -> TicketHistory {
    let mut history = TicketHistory::default();
    if ticket_id.is_empty() {
        return history;
    }
    let mut start: Option<Item> = None;
    loop {
        let mut q = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .index_name("ticket-index")
            .key_condition_expression("team_id = :tid AND ticket_id = :ticket")
            .expression_attribute_values(":tid", attr_s(team_id))
            .expression_attribute_values(":ticket", attr_s(ticket_id));
        if let Some(k) = start.take() {
            q = q.set_exclusive_start_key(Some(k));
        }
        match q.send().await {
            Ok(out) => {
                for item in out.items() {
                    let r = prior_run(item);
                    if r.run_id != current_run_id && !r.run_id.is_empty() {
                        history.runs.push(r);
                    }
                }
                match out.last_evaluated_key() {
                    Some(k) => start = Some(k.clone()),
                    None => break,
                }
            }
            Err(e) => {
                warn!(ticket_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not read the ticket's earlier runs");
                break;
            }
        }
    }
    // ULIDs sort by creation time.
    history.runs.sort_by(|a, b| a.run_id.cmp(&b.run_id));

    if let Ok(out) = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&ticket_state::sk(ticket_id)))
        .send()
        .await
    {
        if let Some(item) = out.item() {
            history.notes = ticket_state::parse_notes(&s(item, ticket_state::FIELD_NOTES));
            history.decisions =
                ticket_state::parse_decisions(&s(item, ticket_state::FIELD_DECISIONS));
            if s(item, ticket_state::FIELD_STATE) == ticket_state::STATE_DONE {
                history.done_hash = s(item, ticket_state::FIELD_DONE_HASH);
            }
        }
    }
    history
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The planner's decision on the rerun: a code change that forces one
    /// record active (the same change people closed before).
    fn override_plan(history_check: &str) -> FixDecision {
        FixDecision {
            fix_location: FixLocation::Code,
            cause: "The class is inactive because its term is not on the web.".into(),
            history_check: history_check.into(),
            ..Default::default()
        }
    }

    fn closed_with_lesson(hash: &str) -> TicketHistory {
        TicketHistory {
            runs: vec![
                PriorRun {
                    run_id: "01A".into(),
                    status: "cancelled".into(),
                    repo: "acme/pipelines".into(),
                    pr_number: 11,
                    pr_outcome: "closed".into(),
                    cancel_reason: "pr_closed_by_human".into(),
                    context_hash: hash.into(),
                    ..Default::default()
                },
                PriorRun {
                    run_id: "01B".into(),
                    status: "completed".into(),
                    repo: "acme/pipelines".into(),
                    pr_number: 12,
                    lesson: "When a class is missing from the site, check whether its term is \
                             available on the web in the booking system first."
                        .into(),
                    lesson_where: "the booking system".into(),
                    context_hash: hash.into(),
                    ..Default::default()
                },
            ],
            notes: vec![],
            done_hash: String::new(),
            prs: vec![],
            decisions: vec![],
        }
    }

    /// The invariant: a ticket whose earlier CoderHelm PR was closed unmerged,
    /// with an outside-code lesson, never gets another PR with the same change
    /// while the ticket is unchanged — whatever the planner says.
    #[test]
    fn a_closed_pr_with_an_outside_lesson_never_gets_a_second_pr() {
        let h = closed_with_lesson("h1");
        for check in ["", "I think the override is still the quickest way."] {
            match h.guard(Some(&override_plan(check)), "h1") {
                Guard::Stop {
                    route, decision, ..
                } => {
                    assert_eq!(route, Route::OutsideOnly);
                    assert!(decision.cause.contains("#11"));
                    assert!(decision.cause.contains("booking system"));
                    assert_eq!(
                        decision.outside_change.as_ref().map(|c| c.system.as_str()),
                        Some("the booking system")
                    );
                }
                Guard::Proceed => panic!("a second PR would be opened"),
            }
        }
        // A plan without any decision doesn't slip through either.
        assert!(matches!(h.guard(None, "h1"), Guard::Proceed));
        assert_eq!(fix_location::route(None), Route::Ask);
    }

    #[test]
    fn a_changed_ticket_may_code_again_only_when_the_plan_answers_the_history() {
        let h = closed_with_lesson("h1");
        assert!(matches!(
            h.guard(Some(&override_plan("")), "h2"),
            Guard::Stop { .. }
        ));
        assert_eq!(
            h.guard(
                Some(&override_plan(
                    "The ticket now asks for a new report column; the earlier fix was in the booking system."
                )),
                "h2"
            ),
            Guard::Proceed
        );
    }

    #[test]
    fn a_closed_pr_alone_needs_an_answer_before_another_pr() {
        let mut h = closed_with_lesson("h1");
        h.runs.truncate(1);
        match h.guard(Some(&override_plan("")), "h1") {
            Guard::Stop { route, .. } => assert_eq!(route, Route::Ask),
            Guard::Proceed => panic!("closed PR ignored"),
        }
        assert_eq!(
            h.guard(
                Some(&override_plan("The reviewer asked for a smaller change.")),
                "h1"
            ),
            Guard::Proceed
        );
    }

    #[test]
    fn a_person_saying_it_was_fixed_elsewhere_stops_a_code_plan() {
        let h = TicketHistory {
            runs: vec![],
            notes: vec![Note {
                author: "a person".into(),
                text: "Fixed this in the booking system, the term is on the web now.".into(),
                intent: Intent::FixElsewhereInfo,
                at: "2026-01-01T00:00:00Z".into(),
            }],
            done_hash: "h1".into(),
            prs: vec![],
            decisions: vec![],
        };
        match h.guard(Some(&override_plan("")), "h1") {
            Guard::Stop {
                route, decision, ..
            } => {
                assert_eq!(route, Route::Ask);
                assert!(decision.cause.contains("Fixed this in the booking system"));
            }
            Guard::Proceed => panic!("note ignored"),
        }
        // The ticket was edited after it was marked done: a plan that
        // answers the note may code.
        assert_eq!(
            h.guard(
                Some(&override_plan("The new text asks for an export column.")),
                "h2"
            ),
            Guard::Proceed
        );
    }

    #[test]
    fn a_ticket_without_history_is_untouched() {
        let h = TicketHistory::default();
        assert_eq!(h.guard(Some(&override_plan("")), "h1"), Guard::Proceed);
        assert_eq!(h.planner_note(), "");
    }

    #[test]
    fn the_planner_is_told_the_history_and_the_digest_moves_with_it() {
        let h = closed_with_lesson("h1");
        let note = h.planner_note();
        assert!(note.contains("pull request #11 (acme/pipelines) closed WITHOUT merging"));
        assert!(note.contains("Lesson the team recorded"));
        let mut with_comment = h.clone();
        with_comment.runs[0].pr_comments = vec![(
            "a-person".into(),
            "The fix is a setting in the booking system.".into(),
        )];
        assert!(with_comment
            .planner_note()
            .contains("a-person wrote on that PR: The fix is a setting"));
        assert_ne!(with_comment.digest(), h.digest());
        assert!(note.contains("history_check"));
        let mut h2 = h.clone();
        h2.notes.push(Note {
            author: "a person".into(),
            text: "resolved".into(),
            intent: Intent::FixElsewhereInfo,
            at: "t".into(),
        });
        assert_ne!(h.digest(), h2.digest());
        assert_eq!(h.digest(), closed_with_lesson("h1").digest());
    }
}

#[cfg(test)]
mod fit_notes_tests {
    use super::{fit_notes, NOTE_FLOOR};

    #[test]
    fn comments_are_whole_when_they_fit() {
        let t = vec!["a".repeat(5_000), "b".repeat(8_000)];
        assert_eq!(fit_notes(t.clone(), 60_000), t);
    }

    #[test]
    fn the_oldest_are_shortened_first() {
        let t = vec!["a".repeat(1_000), "b".repeat(1_000), "c".repeat(1_000)];
        // 3,000 characters, 500 over: the oldest gives up 500.
        let out = fit_notes(t.clone(), 2_500);
        assert!(out[0].starts_with(&"a".repeat(500)));
        assert!(out[0].contains("500 characters of this older comment left out"));
        assert_eq!(out[1..], t[1..], "newer comments stay whole");
        // Far over: every older comment goes down to the floor before the
        // newest is touched.
        let out = fit_notes(t, 1_200);
        assert!(
            out[0].starts_with(&"a".repeat(NOTE_FLOOR))
                && !out[0].starts_with(&"a".repeat(NOTE_FLOOR + 1))
        );
        assert!(
            out[1].starts_with(&"b".repeat(NOTE_FLOOR))
                && !out[1].starts_with(&"b".repeat(NOTE_FLOOR + 1))
        );
        assert!(out[2].starts_with(&"c".repeat(600)));
    }
}

#[cfg(test)]
mod traced_guard_tests {
    use super::*;
    use crate::passes::fix_location::{FixEvidence, Producer};

    fn outside_before(hash: &str) -> TicketHistory {
        TicketHistory {
            runs: vec![PriorRun {
                run_id: "01B".into(),
                status: "needs_outside_change".into(),
                repo: "acme/pipelines".into(),
                fix_location: "outside_code".into(),
                lesson: "The fix is in another ticket.".into(),
                lesson_where: "another ticket".into(),
                context_hash: hash.into(),
                ..Default::default()
            }],
            notes: vec![],
            done_hash: String::new(),
            prs: vec![],
            decisions: vec![],
        }
    }

    fn plan(kind: &str, scope: &str, history_check: &str) -> FixDecision {
        FixDecision {
            fix_location: FixLocation::Code,
            cause: "A sync merges records that differ by place and overwrites the place.".into(),
            evidence: FixEvidence {
                data_shows: "Items at two places share one parent.".into(),
                producer: Producer {
                    kind: kind.into(),
                    where_: "acme/pipelines:dags/sync.py:1230 sync_groups()".into(),
                },
                scope: scope.into(),
                ..Default::default()
            },
            history_check: history_check.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_traced_code_decision_supersedes_an_earlier_outside_conclusion() {
        let h = outside_before("h1");
        let answer = "The earlier runs deferred to a display fix; the trace shows the sync writes the wrong value.";
        assert_eq!(
            h.guard(Some(&plan("code", "41 parents, 7027 items", answer)), "h1"),
            Guard::Proceed
        );
        // Untraced, unmeasured or unanswered: the history still holds it.
        for p in [
            plan("", "41 parents", answer),
            plan("code", "", answer),
            plan("code", "41 parents", ""),
            plan("source", "41 parents", answer),
        ] {
            assert!(
                matches!(h.guard(Some(&p), "h1"), Guard::Stop { .. }),
                "{p:?}"
            );
        }
    }
}

#[cfg(test)]
mod earlier_pr_tests {
    use super::*;

    fn closed(discussed: bool) -> EarlierPr {
        EarlierPr {
            repo: "acme/infra".into(),
            number: 41,
            title: "Add the reporting user's secret".into(),
            author: "coderhelm-bot".into(),
            branch: "feat/abc-12-reporting-user".into(),
            state: "closed".into(),
            created_at: "2026-01-10T10:00:00Z".into(),
            body: "Adds the secret, mirroring the vendor user's.".into(),
            comments: if discussed {
                vec![("a-reviewer".into(), "Split the user out first.".into())]
            } else {
                vec![]
            },
            closed_by: "a-maintainer".into(),
            closed_at: "2026-01-11T09:30:00Z".into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_pr_is_named_by_its_number_or_link_only() {
        assert!(names_pr("see #41.", 41));
        assert!(names_pr("https://github.com/acme/infra/pull/41", 41));
        assert!(!names_pr("see #410", 41));
        assert!(!names_pr("issue 41", 41));
        assert!(!names_pr("#0", 0));
    }

    #[test]
    fn the_planner_reads_each_earlier_pr_and_how_it_ended() {
        let h = TicketHistory {
            prs: vec![closed(false)],
            ..Default::default()
        };
        assert!(!h.is_empty());
        let note = h.planner_note();
        assert!(note.contains(
            "- #41 in acme/infra \"Add the reporting user's secret\" by coderhelm-bot (branch `feat/abc-12-reporting-user`), opened 2026-01-10: closed WITHOUT merging by a-maintainer on 2026-01-11 (close event); nobody wrote a reason on it."
        ), "{note}");
        assert!(note.contains("Description: Adds the secret"), "{note}");
        let discussed = TicketHistory {
            prs: vec![closed(true)],
            ..Default::default()
        };
        let note = discussed.planner_note();
        assert!(note.contains("a-reviewer wrote: Split the user out first."));
        assert!(!note.contains("nobody wrote a reason"));
        assert_ne!(h.digest(), discussed.digest());
        assert_ne!(h.digest(), TicketHistory::default().digest());
    }

    #[test]
    fn the_close_event_names_who_and_when() {
        let events = serde_json::json!([
            {"event": "closed", "actor": {"login": "first"}, "created_at": "2026-01-01T00:00:00Z"},
            {"event": "reopened", "actor": {"login": "first"}, "created_at": "2026-01-02T00:00:00Z"},
            {"event": "closed", "actor": {"login": "second"}, "created_at": "2026-01-03T00:00:00Z"},
            {"event": "labeled", "actor": {"login": "x"}, "created_at": "2026-01-04T00:00:00Z"}
        ]);
        assert_eq!(
            close_event(&events),
            Some(("second".into(), "2026-01-03T00:00:00Z".into()))
        );
        assert_eq!(close_event(&serde_json::json!([])), None);
    }

    #[test]
    fn reviews_and_threads_are_kept_and_coderhelms_own_notes_are_not() {
        let comments = vec![
            serde_json::json!({"user": {"login": "coderhelm[bot]"}, "body": "Working on it"}),
            serde_json::json!({"user": {"login": "a-person"}, "body": "Why a new schema?"}),
            serde_json::json!({"user": {"login": "some-ci[bot]", "type": "Bot"}, "body": "Plan: 1 to add"}),
        ];
        let c = others_comments(&comments, 8);
        assert_eq!(c.len(), 2, "{c:?}");
        assert_eq!(c[0].0, "a-person");
        let reviews = serde_json::json!([
            {"user": {"login": "r1"}, "state": "COMMENTED", "body": ""},
            {"user": {"login": "r2"}, "state": "CHANGES_REQUESTED", "body": ""},
            {"user": {"login": "r3"}, "state": "COMMENTED", "body": "Use the existing module."}
        ]);
        let r = reviews_of(&reviews, 6);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].1, "CHANGES_REQUESTED");
        let threads = vec![
            serde_json::json!({"user": {"login": "r3"}, "path": "a.tf", "body": "Mirror the vendor block."}),
        ];
        assert_eq!(threads_of(&threads, 10)[0].1, "a.tf");
    }

    #[test]
    fn a_run_records_pr_counts_as_an_earlier_pr_once() {
        let mut h = TicketHistory {
            prs: vec![closed(false)],
            ..Default::default()
        };
        h.runs.push(PriorRun {
            run_id: "01A".into(),
            repo: "acme/infra".into(),
            pr_number: 41,
            ..Default::default()
        });
        h.runs.push(PriorRun {
            run_id: "01B".into(),
            repo: "acme/pipelines".into(),
            pr_number: 7,
            ..Default::default()
        });
        assert_eq!(
            h.earlier_pr_numbers(),
            vec![("acme/infra".into(), 41), ("acme/pipelines".into(), 7)]
        );
        let g = h.plan_gaps("Nothing about earlier work.");
        assert_eq!(g.len(), 2, "{g:?}");
        assert!(g[0].contains("#41 in acme/infra, #7 in acme/pipelines"));
        assert!(h
            .plan_gaps("#7 kept; #41 closed by a-maintainer on 2026-01-11, asking why.")
            .is_empty());
    }

    /// Answers given in CoderHelm on an earlier run are read with it (the
    /// run merges them into its own, see `passes::run_passes`).
    #[test]
    fn an_earlier_runs_answers_are_read() {
        let mut item = Item::new();
        item.insert("run_id".into(), attr_s("01Q"));
        item.insert("status".into(), attr_s("retried"));
        item.insert(
            "answers".into(),
            attr_s(
                r#"[{"question_id":"q1","question":"Which environments get the new limit?","answer":"Production only","author":"Pat Doe","answered_at":"2026-10-27T10:00:00Z"}]"#,
            ),
        );
        let r = prior_run(&item);
        assert_eq!(r.answers.len(), 1);
        assert_eq!(r.answers[0].answer, "Production only");
        item.remove("answers");
        assert!(prior_run(&item).answers.is_empty());
    }
}
