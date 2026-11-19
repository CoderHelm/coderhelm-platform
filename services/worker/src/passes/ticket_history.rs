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
    /// Every pull request the run opened, each with its repository.
    pub pr_list: Vec<common::run_prs::RunPr>,
    /// The open pull request the run ended as covered by, if it did.
    pub covered_by: Option<common::stop_rules::covering::Record>,
    /// A person asked for the run after the ticket's pull request merged
    /// (`common::merged_ticket`).
    pub after_merge: bool,
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
    /// Earlier attempts at the same feature in the run's repository, found
    /// before planning (see `feature::prior`).
    pub prior: crate::passes::feature::prior::Found,
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
    /// The branch it targets (for a merged one: the branch it merged into).
    pub base: String,
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
    /// The ticket's own pull request: its title or branch names the ticket,
    /// or an earlier run of the ticket opened it. One that names the ticket
    /// only in its description is context, not the ticket's.
    pub for_ticket: bool,
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
    /// Whether an earlier run of this ticket opened `repo#number`.
    fn opened_by_run(&self, repo: &str, number: u64) -> bool {
        self.runs.iter().any(|r| {
            r.pr_list
                .iter()
                .any(|p| p.pr_number == number && p.repo.eq_ignore_ascii_case(repo))
        })
    }

    /// CoderHelm's own pull request for this ticket that is still open in
    /// `repo` (an earlier run opened it), newest first: a re-run continues
    /// on it instead of opening another.
    pub fn own_open_pr(&self, repo: &str) -> Option<&EarlierPr> {
        self.prs
            .iter()
            .filter(|p| {
                p.state == "open"
                    && p.repo.eq_ignore_ascii_case(repo)
                    && super::is_own_login(&p.author)
                    && !p.branch.is_empty()
                    && self.opened_by_run(&p.repo, p.number)
            })
            .max_by(|a, b| {
                a.created_at
                    .cmp(&b.created_at)
                    .then(a.number.cmp(&b.number))
            })
    }

    /// CoderHelm's own open pull requests for this ticket in repositories
    /// this run's plan no longer changes: superseded, with a note, once the
    /// new pull request is open.
    pub fn own_open_prs_elsewhere(&self, repos: &[String]) -> Vec<&EarlierPr> {
        self.prs
            .iter()
            .filter(|p| {
                p.state == "open"
                    && !repos.iter().any(|r| r.eq_ignore_ascii_case(&p.repo))
                    && super::is_own_login(&p.author)
                    && self.opened_by_run(&p.repo, p.number)
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty() && self.notes.is_empty() && self.prs.is_empty()
    }

    /// Every merged pull request for the ticket: the ticket's own ones read
    /// from GitHub (any author, any base branch), and any an earlier run's
    /// record says merged that couldn't be read. `repo` (`owner/name`), when
    /// given, keeps only that repository's (a GitHub issue number names an
    /// issue only within its own repository).
    fn merged_prs(&self, repo: Option<&str>) -> Vec<common::merged_ticket::MergedPr> {
        let mut out: Vec<common::merged_ticket::MergedPr> = self
            .prs
            .iter()
            .filter(|p| p.state == "merged" && p.for_ticket && in_repo(repo, &p.repo))
            .map(|p| common::merged_ticket::MergedPr {
                repo: p.repo.clone(),
                number: p.number,
                base: p.base.clone(),
                merged_at: p.closed_at.clone(),
                revert: common::merged_ticket::is_revert(&p.title, &p.branch),
            })
            .collect();
        for r in &self.runs {
            let merged = r.pr_outcome == "merged" || r.status == common::run_status::MERGED;
            let read = self
                .prs
                .iter()
                .any(|p| p.number == r.pr_number && p.repo.eq_ignore_ascii_case(&r.repo));
            if merged && r.pr_number > 0 && r.repo.contains('/') && in_repo(repo, &r.repo) && !read
            {
                out.push(common::merged_ticket::MergedPr {
                    repo: r.repo.clone(),
                    number: r.pr_number,
                    ..Default::default()
                });
            }
        }
        out
    }

    /// Earlier runs a person asked for after the ticket's pull request
    /// merged, by id; only `repo`'s (`owner/name`) when given, as in
    /// [`Self::merged_prs`].
    fn allowed_after_merge(&self, repo: Option<&str>) -> Vec<&str> {
        self.runs
            .iter()
            .filter(|r| r.after_merge && in_repo(repo, &r.repo))
            .map(|r| r.run_id.as_str())
            .collect()
    }

    /// The merged pull requests a new run on the ticket in `repo`
    /// (`owner/name`) is checked against, newest first, unless a person
    /// asks for it (`common::merged_ticket::blocking`).
    /// A GitHub issue's number names it only within its repository, so only
    /// that repository's pull requests and runs count; a Jira key counts
    /// across all the team's repositories.
    pub fn blocking_merge(
        &self,
        source: &common::messages::TicketSource,
        repo: &str,
    ) -> Vec<common::merged_ticket::MergedPr> {
        let scope = matches!(source, common::messages::TicketSource::Github).then_some(repo);
        let merged = self.merged_prs(scope);
        let allowed = self.allowed_after_merge(scope);
        common::merged_ticket::blocking(&merged, &allowed)
            .into_iter()
            .cloned()
            .collect()
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
        // An earlier "outside the code" conclusion is judged again under
        // today's rules: one whose own trace shows a working value our code
        // passes over was a code fix all along, and never stops a plan.
        let conclusion = self
            .outside_conclusion()
            .filter(|c| fix_location::working_alternative(&c.decision).is_none());
        if let Some(c) = conclusion {
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
        pr_list: common::run_prs::from_item(item),
        covered_by: serde_json::from_str(&s(item, "covered_by")).ok(),
        after_merge: item
            .get(common::merged_ticket::FIELD_AFTER_MERGE)
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
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

/// Whether `repo` is `scope` (`owner/name`), or there is no scope.
fn in_repo(scope: Option<&str>, repo: &str) -> bool {
    scope.is_none_or(|want| want.eq_ignore_ascii_case(repo))
}

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

/// A GitHub issue: its repository (`owner/name`) and number.
pub type Issue<'a> = (&'a str, u64);

/// Words that make a GitHub closing reference.
const CLOSING_WORDS: [&str; 9] = [
    "close", "closes", "closed", "fix", "fixes", "fixed", "resolve", "resolves", "resolved",
];

/// Whether a pull request in `repo` is for the GitHub issue `issue`: it is
/// in the issue's own repository, and its title carries `#<n>` or its
/// description has a closing reference to the issue ([`closes_issue`]).
/// Any other mention of the issue is context, not a link.
fn links_issue(issue: Option<Issue>, repo: &str, title: &str, body: &str) -> bool {
    issue.is_some_and(|(own, n)| {
        own.eq_ignore_ascii_case(repo) && (title_refs_issue(title, n) || closes_issue(body, own, n))
    })
}

/// `#<n>` in a title as a whole reference: not `#<n>0`, and not another
/// repository's (`acme/api#<n>`).
fn title_refs_issue(title: &str, n: u64) -> bool {
    let want = format!("#{n}");
    let mut from = 0;
    while let Some(i) = title[from..].find(&want) {
        let at = from + i;
        let before = title[..at].chars().next_back();
        let after = title[at + want.len()..].chars().next();
        let whole_before = before
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '_' | '.')));
        let whole_after = after.is_none_or(|c| !c.is_ascii_alphanumeric());
        if whole_before && whole_after {
            return true;
        }
        from = at + want.len();
    }
    false
}

/// A GitHub closing reference to issue `n` of `repo` in a description: a
/// closing word ([`CLOSING_WORDS`], any case) followed by `#<n>`,
/// `owner/name#<n>` or the issue's URL.
fn closes_issue(body: &str, repo: &str, n: u64) -> bool {
    let lower = body.to_ascii_lowercase();
    let repo = repo.to_ascii_lowercase();
    let refs = [
        format!("#{n}"),
        format!("{repo}#{n}"),
        format!("https://github.com/{repo}/issues/{n}"),
    ];
    let words: Vec<&str> = lower.split_whitespace().collect();
    words.windows(2).any(|w| {
        let word = w[0].trim_matches(|c: char| !c.is_ascii_alphanumeric());
        let reference = w[1].trim_end_matches([',', '.', ')', ';', ':']);
        CLOSING_WORDS.contains(&word) && refs.iter().any(|r| r == reference)
    })
}

/// The pull requests to read for the ticket, at most [`MAX_PRS`], from the
/// team's repositories' listed pull requests (`(owner/name, list)`): first
/// every one an earlier run opened, then the ones that are the ticket's own
/// (the title or branch names the ticket, or they link the GitHub `issue`),
/// then the ones that name it only in their description. A ticket many
/// descriptions mention keeps its own pull requests in the list. Each comes
/// with whether it is the ticket's own ([`EarlierPr::for_ticket`]), decided
/// here from the full listed description.
fn candidates(
    history: &TicketHistory,
    listed: &[(String, serde_json::Value)],
    ticket_id: &str,
    issue: Option<Issue<'_>>,
) -> Vec<(String, u64, bool)> {
    let mut found: Vec<(String, u64)> = Vec::new();
    // Every pull request an earlier run opened, in its own repository (a
    // multi-repo run's record pairs its repo with only one of them).
    for p in history.runs.iter().flat_map(|r| r.pr_list.iter()) {
        if p.pr_number > 0
            && p.repo.contains('/')
            && !found.contains(&(p.repo.clone(), p.pr_number))
        {
            found.push((p.repo.clone(), p.pr_number));
        }
    }
    fn text(v: &serde_json::Value) -> &str {
        v.as_str().unwrap_or("")
    }
    let mut mentioned: Vec<(String, u64)> = Vec::new();
    let mut reverts: Vec<((String, u64), u64)> = Vec::new();
    if !ticket_id.is_empty() {
        for (repo, v) in listed {
            for p in v.as_array().into_iter().flatten() {
                let number = p["number"].as_u64().unwrap_or(0);
                let (title, branch, body) =
                    (text(&p["title"]), text(&p["head"]["ref"]), text(&p["body"]));
                let key = (repo.clone(), number);
                if number == 0 || found.contains(&key) || mentioned.contains(&key) {
                    continue;
                }
                if super::mentions_ticket(title, ticket_id)
                    || super::mentions_ticket(branch, ticket_id)
                    || links_issue(issue, repo, title, body)
                {
                    found.push(key);
                } else {
                    if common::merged_ticket::is_revert(title, branch) {
                        if let Some(target) = reverted_pr(repo, branch, body) {
                            reverts.push((key.clone(), target));
                        }
                    }
                    if super::mentions_ticket(body, ticket_id) {
                        mentioned.push(key);
                    }
                }
            }
        }
    }
    // A revert of the ticket's own pull request is the ticket's own too
    // (GitHub's Revert button names neither the ticket nor the issue).
    for (key, target) in reverts {
        if let Some(at) = found.iter().position(|f| f.0 == key.0 && f.1 == target) {
            mentioned.retain(|m| m != &key);
            found.insert(at, key);
        }
    }
    let own = found.len();
    found.extend(mentioned);
    found.truncate(MAX_PRS);
    found
        .into_iter()
        .enumerate()
        .map(|(i, (repo, number))| (repo, number, i < own))
        .collect()
}

/// The pull request in `repo` that a revert pull request reverts: from
/// GitHub's Revert-button branch (`revert-<n>-…`) or its description
/// (`Reverts #<n>`, `Reverts owner/name#<n>` naming the same repository).
fn reverted_pr(repo: &str, branch: &str, body: &str) -> Option<u64> {
    if let Some(n) = branch
        .strip_prefix("revert-")
        .and_then(|rest| rest.split_once('-'))
        .and_then(|(n, _)| n.parse().ok())
    {
        return Some(n);
    }
    let lower = body.to_ascii_lowercase();
    let own = format!("{}#", repo.to_ascii_lowercase());
    let words: Vec<&str> = lower.split_whitespace().collect();
    words.windows(2).find_map(|w| {
        let word = w[0].trim_matches(|c: char| !c.is_ascii_alphanumeric());
        let reference = w[1].trim_end_matches([',', '.', ')', ';', ':']);
        let number = reference
            .strip_prefix(&own)
            .or_else(|| reference.strip_prefix('#'))?;
        (word == "reverts").then(|| number.parse().ok()).flatten()
    })
}

/// Adds every pull request for the ticket in `repos` (`owner/name`, the
/// team's repositories, searched in order): the ones the ticket's runs
/// opened, any whose title, branch or description names the ticket, and,
/// for a GitHub issue (`issue`), any in its repository that links it
/// ([`links_issue`]).
/// Each is read in full: description, comments, reviews, inline review
/// threads, and how it ended (GitHub's `closed` event: who and when; for a
/// merged one, who merged it). Best-effort, bounded by [`MAX_PRS`].
pub async fn add_earlier_prs(
    history: &mut TicketHistory,
    github: &crate::clients::github::GitHubClient,
    repos: &[String],
    ticket_id: &str,
    issue: Option<Issue<'_>>,
) {
    let mut listed: Vec<(String, serde_json::Value)> = Vec::new();
    if !ticket_id.is_empty() {
        for repo in repos.iter().take(MAX_REPOS) {
            let Some((owner, name)) = repo.split_once('/') else {
                continue;
            };
            match github.list_pull_requests(owner, name, "all", 100).await {
                Ok(v) => listed.push((repo.clone(), v)),
                Err(e) => {
                    warn!(repo, error = %e, "Could not list pull requests for the ticket's history")
                }
            }
        }
    }
    for (repo, number, for_ticket) in candidates(history, &listed, ticket_id, issue) {
        if let Some(mut p) = read_pr(github, &repo, number).await {
            p.for_ticket = for_ticket;
            history.prs.push(p);
        }
    }
    history.prs.sort_by(|a, b| a.created_at.cmp(&b.created_at));
}

/// One pull request read in full. None when it can't be read.
pub(crate) async fn read_pr(
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
        base: text(&pr["base"]["ref"]),
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

    fn run_with_prs(id: &str, prs: &[(&str, u64)]) -> PriorRun {
        PriorRun {
            run_id: id.into(),
            pr_list: prs
                .iter()
                .map(|(repo, n)| common::run_prs::RunPr {
                    repo: repo.to_string(),
                    pr_number: *n,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Whether `p`, listed alone in its repository, is read as the ticket's
    /// own pull request.
    fn linked(h: &TicketHistory, p: &EarlierPr, ticket_id: &str, issue: Option<Issue>) -> bool {
        let listed = vec![(
            p.repo.clone(),
            serde_json::json!([{
                "number": p.number,
                "title": p.title,
                "head": {"ref": p.branch},
                "body": p.body,
            }]),
        )];
        candidates(h, &listed, ticket_id, issue)
            .iter()
            .any(|(repo, n, own)| *own && *n == p.number && repo == &p.repo)
    }

    /// "Closes #42" past the 6000 characters of a description kept when
    /// the pull request is read still marks it as the issue's own: the
    /// link is decided from the full listed description.
    #[test]
    fn a_closing_reference_deep_in_a_long_description_counts() {
        let body = format!("{}\n\nCloses #42", "Checklist item. ".repeat(500));
        assert!(body.find("Closes").unwrap() > 6000);
        let listed = vec![(
            "acme/api".to_string(),
            serde_json::json!([{
                "number": 7,
                "title": "Fix login",
                "head": {"ref": "pat/login"},
                "body": body,
            }]),
        )];
        let found = candidates(
            &TicketHistory::default(),
            &listed,
            "GH-42",
            Some(("acme/api", 42)),
        );
        assert_eq!(found, vec![("acme/api".to_string(), 7, true)]);
    }

    fn earlier(repo: &str, n: u64, author: &str, state: &str, at: &str) -> EarlierPr {
        EarlierPr {
            repo: repo.into(),
            number: n,
            author: author.into(),
            branch: format!("coderhelm/t-{n}"),
            state: state.into(),
            created_at: at.into(),
            ..Default::default()
        }
    }

    /// Every merged PR for the ticket stops a new run: read from GitHub
    /// (any author, its base branch kept), or named merged by a run record
    /// when GitHub couldn't be read; never one listed twice.
    #[test]
    fn merged_pull_requests_come_from_github_and_the_run_records() {
        let mut integration = earlier("acme/web", 5, "pat", "merged", "2026-10-01");
        integration.base = "integration".into();
        integration.closed_at = "2026-10-02T08:00:00Z".into();
        integration.for_ticket = true;
        let h = TicketHistory {
            runs: vec![
                PriorRun {
                    run_id: "01A".into(),
                    repo: "acme/web".into(),
                    pr_number: 5,
                    pr_outcome: "merged".into(),
                    ..Default::default()
                },
                PriorRun {
                    run_id: "01B".into(),
                    repo: "acme/api".into(),
                    pr_number: 3,
                    status: "merged".into(),
                    after_merge: true,
                    ..Default::default()
                },
                PriorRun {
                    run_id: "01C".into(),
                    repo: "acme/api".into(),
                    pr_number: 4,
                    pr_outcome: "closed".into(),
                    ..Default::default()
                },
            ],
            prs: vec![
                integration,
                earlier("acme/web", 9, "coderhelm[bot]", "open", "2026-10-03"),
            ],
            ..Default::default()
        };
        let merged = h.merged_prs(None);
        assert_eq!(merged.len(), 2, "{merged:?}");
        assert_eq!(merged[0].subject(), "acme/web#5");
        assert_eq!(merged[0].base, "integration");
        assert_eq!(merged[0].merged_at, "2026-10-02T08:00:00Z");
        assert_eq!(merged[1].subject(), "acme/api#3");
        assert_eq!(h.allowed_after_merge(None), vec!["01B"]);
    }

    /// A run id (ULID) started at `rfc3339`.
    fn run_id_at(rfc3339: &str) -> String {
        let t = chrono::DateTime::parse_from_rfc3339(rfc3339).unwrap();
        ulid::Ulid::from_parts(t.timestamp_millis() as u64, 7).to_string()
    }

    /// GitHub issue numbers repeat across repositories: acme/web#42 and
    /// acme/api#42 are both `GH-42`. A merge for acme/web's issue stops a
    /// new run on acme/web's, never one on acme/api's; a person's go-ahead
    /// on acme/api's doesn't let acme/web's run either. A Jira key is one
    /// ticket across the team's repositories.
    #[test]
    fn a_github_issue_is_stopped_only_by_its_own_repositorys_merge() {
        use common::messages::TicketSource;
        let mut web = earlier("acme/web", 7, "coderhelm[bot]", "merged", "2026-11-01");
        web.title = "Fix the login redirect (GH-42)".into();
        web.closed_at = "2026-11-02T10:00:00Z".into();
        web.for_ticket = true;
        let h = TicketHistory {
            runs: vec![
                PriorRun {
                    run_id: run_id_at("2026-10-20T09:00:00Z"),
                    repo: "acme/web".into(),
                    pr_number: 6,
                    status: "merged".into(),
                    ..Default::default()
                },
                PriorRun {
                    run_id: run_id_at("2026-11-05T09:00:00Z"),
                    repo: "acme/api".into(),
                    after_merge: true,
                    ..Default::default()
                },
            ],
            prs: vec![web],
            ..Default::default()
        };
        assert_eq!(h.blocking_merge(&TicketSource::Github, "acme/api"), vec![]);
        assert_eq!(
            h.blocking_merge(&TicketSource::Github, "acme/web")
                .first()
                .map(|p| p.subject()),
            Some("acme/web#7".to_string())
        );
        let mut jira = h.clone();
        jira.runs.pop();
        assert_eq!(
            jira.blocking_merge(&TicketSource::Jira, "acme/api")
                .first()
                .map(|p| p.subject()),
            Some("acme/web#7".to_string())
        );
    }

    /// The ticket's own pull request is one a run of the ticket opened, or
    /// one whose title or branch names it. The PR for WEB-43 that says
    /// "Prepares the API for WEB-42" in its description merged: a run on
    /// WEB-42 is not stopped by it.
    #[test]
    fn a_pull_request_naming_the_ticket_only_in_its_description_never_stops_a_run() {
        use common::messages::TicketSource;
        let mut h = TicketHistory {
            runs: vec![run_with_prs("01A", &[("acme/app", 50)])],
            ..Default::default()
        };
        let mut prep = earlier("acme/app", 43, "pat", "merged", "2026-11-01");
        prep.title = "WEB-43: API prep".into();
        prep.branch = "feature/web-43".into();
        prep.body = "Prepares the API for WEB-42".into();
        prep.closed_at = "2026-11-02T10:00:00Z".into();
        prep.for_ticket = linked(&h, &prep, "WEB-42", None);
        assert!(!prep.for_ticket);
        h.prs.push(prep.clone());
        assert_eq!(h.blocking_merge(&TicketSource::Jira, "acme/app"), vec![]);

        let by_title = EarlierPr {
            title: "WEB-42: rate limit the API".into(),
            ..prep.clone()
        };
        let by_branch = EarlierPr {
            branch: "pat/web-42-limits".into(),
            ..prep.clone()
        };
        let by_run = EarlierPr {
            number: 50,
            ..prep.clone()
        };
        for p in [&by_title, &by_branch, &by_run] {
            assert!(linked(&h, p, "WEB-42", None), "{p:?}");
        }
        let mut stopped = h.clone();
        let mut own = by_title.clone();
        own.for_ticket = linked(&h, &own, "WEB-42", None);
        stopped.prs.push(own);
        assert_eq!(
            stopped
                .blocking_merge(&TicketSource::Jira, "acme/app")
                .first()
                .map(|p| p.subject()),
            Some("acme/app#43".to_string())
        );
    }

    /// A person's PR on a GitHub issue links it the way GitHub does: `#42`
    /// in its title, or a closing reference ("Fixes #42") in its
    /// description. Either one, merged (into `develop` too), stops a run on
    /// acme/api's GH-42. "prepares for #42" is only a mention, and a
    /// closing reference from another repository's PR names another
    /// repository's issue 42.
    #[test]
    fn a_github_closing_reference_or_title_number_marks_the_issues_pr() {
        use common::messages::TicketSource;
        let issue = Some(("acme/api", 42));
        let stops = |title: &str, body: &str, repo: &str, base: &str| {
            let mut p = earlier(repo, 7, "pat", "merged", "2026-11-01");
            p.title = title.into();
            p.branch = "pat/login".into();
            p.body = body.into();
            p.base = base.into();
            p.closed_at = "2026-11-02T10:00:00Z".into();
            let mut h = TicketHistory::default();
            p.for_ticket = linked(&h, &p, "GH-42", issue);
            h.prs.push(p);
            !h.blocking_merge(&TicketSource::Github, "acme/api")
                .is_empty()
        };
        assert!(stops("Fix login (#42)", "", "acme/api", "main"));
        assert!(stops("Fix login", "Fixes #42", "acme/api", "develop"));
        assert!(stops(
            "Fix login",
            "This resolves: acme/api#42.",
            "acme/api",
            "main"
        ));
        assert!(stops(
            "Fix login",
            "Closes https://github.com/acme/api/issues/42",
            "acme/api",
            "main"
        ));
        assert!(!stops(
            "Fix login",
            "This prepares for #42",
            "acme/api",
            "main"
        ));
        assert!(!stops(
            "Fix login",
            "See #42; related to #42",
            "acme/api",
            "main"
        ));
        assert!(!stops("Fix login (#420)", "Fixes #421", "acme/api", "main"));
        assert!(!stops(
            "Bump acme/web#42",
            "Fixes acme/web#42",
            "acme/api",
            "main"
        ));
        assert!(!stops("Fix login (#42)", "Fixes #42", "acme/web", "main"));
    }

    /// WEB-42's PR merged, then GitHub's Revert button undid it: the work
    /// is gone, so a new run goes ahead. When a fix merges after the
    /// revert, that fix stops the next run, and the refusal cites it.
    #[test]
    fn a_reverted_merge_doesnt_stop_the_run_until_a_new_one_merges() {
        use common::messages::TicketSource;
        let pr = |n: u64, title: &str, branch: &str, at: &str| {
            let mut p = earlier("acme/app", n, "pat", "merged", "2026-11-01");
            p.title = title.into();
            p.branch = branch.into();
            p.closed_at = at.into();
            p.for_ticket = true;
            p
        };
        let first = pr(
            12,
            "WEB-42: rate limit the API",
            "pat/web-42",
            "2026-11-02T10:00:00Z",
        );
        let revert = pr(
            13,
            "Revert \"WEB-42: rate limit the API\"",
            "revert-12-pat/web-42",
            "2026-11-03T10:00:00Z",
        );
        let again = pr(
            14,
            "WEB-42: rate limit the API, take two",
            "pat/web-42-b",
            "2026-11-05T10:00:00Z",
        );
        let reverted = TicketHistory {
            prs: vec![first.clone(), revert.clone()],
            ..Default::default()
        };
        assert_eq!(
            reverted.blocking_merge(&TicketSource::Jira, "acme/app"),
            vec![]
        );
        let remerged = TicketHistory {
            prs: vec![first, revert, again],
            ..Default::default()
        };
        assert_eq!(
            remerged
                .blocking_merge(&TicketSource::Jira, "acme/app")
                .first()
                .map(|p| p.subject()),
            Some("acme/app#14".to_string())
        );
    }

    /// Nine pull requests name WEB-42 only in their description ("part of
    /// WEB-42"); the ticket's own merged PR is the oldest of the listed ten.
    /// It is still read, so a run on WEB-42 stops.
    #[test]
    fn the_tickets_own_pull_requests_come_before_description_mentions() {
        let mut list: Vec<serde_json::Value> = (20..29)
            .rev()
            .map(|n| {
                serde_json::json!({
                    "number": n,
                    "title": format!("Chore {n}"),
                    "head": {"ref": format!("chore-{n}")},
                    "body": "Part of WEB-42",
                })
            })
            .collect();
        list.push(serde_json::json!({
            "number": 12,
            "title": "WEB-42: rate limit the API",
            "head": {"ref": "pat/limits"},
            "body": "",
        }));
        let listed = vec![("acme/app".to_string(), serde_json::Value::Array(list))];
        let found = candidates(&TicketHistory::default(), &listed, "WEB-42", None);
        assert_eq!(found.len(), MAX_PRS);
        assert_eq!(found[0], ("acme/app".to_string(), 12, true));
        assert!(found[1..].iter().all(|(_, _, own)| !own));
        // A pull request that names another ticket isn't read at all.
        let other = vec![(
            "acme/app".to_string(),
            serde_json::json!([{"number": 5, "title": "WEB-43", "head": {"ref": "x"}, "body": ""}]),
        )];
        assert!(candidates(&TicketHistory::default(), &other, "WEB-42", None).is_empty());
    }

    /// A person's PR acme/api#7 ("Fix login", "Fixes #42") merged, then
    /// GitHub's Revert button undid it: `Revert "Fix login"` on
    /// `revert-7-pat/login`, "Reverts acme/api#7". The revert is read as
    /// the issue's own, so a new run on GH-42 isn't refused. A revert of a
    /// pull request that isn't the issue's is ignored.
    #[test]
    fn a_revert_button_pr_of_the_tickets_pr_is_the_tickets_own() {
        use common::messages::TicketSource;
        let pr = |n: u64, title: &str, branch: &str, body: &str, at: &str| {
            serde_json::json!({
                "number": n,
                "title": title,
                "head": {"ref": branch},
                "body": body,
                "merged_at": at,
            })
        };
        let listed = vec![(
            "acme/api".to_string(),
            serde_json::json!([
                pr(
                    9,
                    "Revert \"Bump deps\"",
                    "revert-3-pat/deps",
                    "Reverts acme/api#3",
                    "x"
                ),
                pr(
                    8,
                    "Revert \"Fix login\"",
                    "revert-7-pat/login",
                    "Reverts acme/api#7",
                    "x"
                ),
                pr(7, "Fix login", "pat/login", "Fixes #42", "x"),
                pr(3, "Bump deps", "pat/deps", "", "x"),
            ]),
        )];
        let issue = Some(("acme/api", 42));
        let found = candidates(&TicketHistory::default(), &listed, "GH-42", issue);
        assert_eq!(
            found,
            vec![
                ("acme/api".to_string(), 8, true),
                ("acme/api".to_string(), 7, true),
            ]
        );
        let read = |n: u64, title: &str, branch: &str, at: &str| {
            let mut p = earlier("acme/api", n, "pat", "merged", "2026-11-01");
            p.title = title.into();
            p.branch = branch.into();
            p.closed_at = at.into();
            p.for_ticket = true;
            p
        };
        let h = TicketHistory {
            prs: vec![
                read(7, "Fix login", "pat/login", "2026-11-02T10:00:00Z"),
                read(
                    8,
                    "Revert \"Fix login\"",
                    "revert-7-pat/login",
                    "2026-11-03T10:00:00Z",
                ),
            ],
            ..Default::default()
        };
        assert_eq!(h.blocking_merge(&TicketSource::Github, "acme/api"), vec![]);
        // Found through the description alone, too; never another
        // repository's pull request.
        assert_eq!(
            reverted_pr("acme/api", "pat/undo", "Reverts acme/api#7"),
            Some(7)
        );
        assert_eq!(reverted_pr("acme/api", "pat/undo", "Reverts #7."), Some(7));
        assert_eq!(
            reverted_pr("acme/api", "pat/undo", "Reverts acme/web#7"),
            None
        );
        assert_eq!(reverted_pr("acme/api", "pat/undo", "See #7"), None);
    }

    /// Seven earlier CoderHelm pull requests fill most of the list; a
    /// person's acme/api#7 ("Fixes #42") merged and #8 reverted it. The
    /// list keeps the revert along with what it reverts, so a new run on
    /// GH-42 isn't refused.
    #[test]
    fn a_revert_survives_the_cut_whenever_its_target_does() {
        use common::messages::TicketSource;
        let history = TicketHistory {
            runs: (20..27)
                .map(|n| run_with_prs(&format!("01{n}"), &[("acme/api", n)]))
                .collect(),
            ..Default::default()
        };
        let listed = vec![(
            "acme/api".to_string(),
            serde_json::json!([
                {"number": 8, "title": "Revert \"Fix login\"",
                 "head": {"ref": "revert-7-pat/login"}, "body": "Reverts acme/api#7"},
                {"number": 7, "title": "Fix login", "head": {"ref": "pat/login"},
                 "body": "Fixes #42"},
            ]),
        )];
        let found = candidates(&history, &listed, "GH-42", Some(("acme/api", 42)));
        assert_eq!(found.len(), MAX_PRS);
        assert!(
            found.contains(&("acme/api".to_string(), 8, true)),
            "{found:?}"
        );
        let mut h = history.clone();
        for (repo, n, own) in found {
            let mut p = earlier(&repo, n, "coderhelm[bot]", "closed", "2026-10-01");
            if n == 7 || n == 8 {
                p.state = "merged".into();
                p.author = "pat".into();
                p.title = if n == 8 {
                    "Revert \"Fix login\""
                } else {
                    "Fix login"
                }
                .into();
                p.branch = if n == 8 {
                    "revert-7-pat/login"
                } else {
                    "pat/login"
                }
                .into();
                p.closed_at = format!("2026-11-0{}T10:00:00Z", n - 5);
            }
            p.for_ticket = own;
            h.prs.push(p);
        }
        assert_eq!(h.blocking_merge(&TicketSource::Github, "acme/api"), vec![]);
    }

    /// A re-run continues on CoderHelm's own open PR in the same repo: the
    /// newest one an earlier run opened, never a person's, a closed one, or
    /// one no run of this ticket opened.
    #[test]
    fn a_rerun_finds_its_own_open_pull_request() {
        let h = TicketHistory {
            runs: vec![
                run_with_prs("01A", &[("acme/web", 5)]),
                run_with_prs("01B", &[("acme/web", 9), ("acme/api", 3)]),
            ],
            prs: vec![
                earlier("acme/web", 5, "coderhelm[bot]", "closed", "2026-10-01"),
                earlier("acme/web", 9, "coderhelm[bot]", "open", "2026-10-02"),
                earlier("acme/web", 11, "pat", "open", "2026-10-03"),
                earlier("acme/web", 12, "coderhelm[bot]", "open", "2026-10-04"),
                earlier("acme/api", 3, "coderhelm[bot]", "open", "2026-10-02"),
            ],
            ..Default::default()
        };
        assert_eq!(h.own_open_pr("acme/web").map(|p| p.number), Some(9));
        assert_eq!(h.own_open_pr("ACME/Api").map(|p| p.number), Some(3));
        assert!(h.own_open_pr("acme/docs").is_none());

        // The plan moved to acme/web only: the api PR is superseded.
        let moved = h.own_open_prs_elsewhere(&["acme/web".to_string()]);
        assert_eq!(
            moved
                .iter()
                .map(|p| (p.repo.as_str(), p.number))
                .collect::<Vec<_>>(),
            [("acme/api", 3)]
        );
        // Both repos still in the plan: nothing to supersede.
        assert!(h
            .own_open_prs_elsewhere(&["acme/web".into(), "acme/api".into()])
            .is_empty());
    }

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
            prior: Default::default(),
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
            prior: Default::default(),
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
            prior: Default::default(),
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
