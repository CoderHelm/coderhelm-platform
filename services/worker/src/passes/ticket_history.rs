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
        self.runs.is_empty() && self.notes.is_empty()
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
             CoderHelm has worked on this ticket before. Answer this history in `history_check` \
             before you plan any code. The default is NOT to open another pull request: if the \
             history says the fix is outside the code, or a person said it was fixed elsewhere, \
             decide `outside_code` (or `unknown`) and reply on the ticket instead.\n",
        );
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
            for n in &self.notes {
                out.push_str(&format!(
                    "- {} {} ({}): {}\n",
                    n.at.get(..16).unwrap_or(&n.at),
                    n.author,
                    n.intent.as_str(),
                    one_line(&common::jira_text::text_of(&n.text), NOTE_CHARS)
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

/// How much of each ticket comment the planner reads: enough for a person's
/// written investigation, not only its first sentence.
const NOTE_CHARS: usize = 2500;

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
