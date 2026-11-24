//! What a person answered on the ticket is a decision, in every run and on
//! every pull request for it.
//!
//! The failure this was written for: a run asked on its ticket for the
//! page's heading and intro copy, with a proposed default. The product
//! owner answered on the ticket ("Skip the heading and intro copy. Just use
//! the title."). That run took the answer as a round on its own pull
//! request. Two days later a new run on the same ticket opened a new pull
//! request: the answer was already marked handled, so the new run never
//! read it, built the intro anyway and listed it under "Decisions pending
//! product input".
//!
//! Now:
//!
//! - [`from_comments`]: every answer and instruction a person wrote on the
//!   ticket is a decision for every run on it, whichever run handled it.
//!   The worker merges them into the run's answers before triage and
//!   planning, and before each round.
//! - [`settle`]: a description never lists an answered item as pending. An
//!   item under a pending heading ("pending", "open questions", "awaiting
//!   product") that an answer settles ([`answers_item`]: it shares the
//!   question's words) is taken out, and the answer is listed under
//!   "Decided on the ticket". When an answer goes against an acceptance
//!   criterion of the ticket, one line says so ([`conflict`]).
//! - [`remember`] keeps a ticket's decisions for the description writer
//!   (`description::save`), which knows the ticket from the description's
//!   own "Ticket:" line ([`ticket_of`]).

use crate::run_questions::Answer;
use crate::ticket_comments::{Class, Comment};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// The heading the answered items are listed under.
pub const DECIDED_HEADING: &str = "### Decided on the ticket";

/// Every answer or instruction a person wrote on the ticket, whatever run
/// read it and whatever state it is in, as answers (oldest first).
pub fn from_comments(comments: &[Comment]) -> Vec<Answer> {
    let mut sorted: Vec<&Comment> = comments
        .iter()
        .filter(|c| matches!(c.class, Some(Class::Answer) | Some(Class::Instruction)))
        .filter(|c| !c.text.trim().is_empty())
        .collect();
    sorted.sort_by(|a, b| a.at.cmp(&b.at));
    sorted
        .into_iter()
        .map(|c| Answer {
            question_id: format!("ticket-comment-{}", c.comment_id),
            question: c.question.trim().to_string(),
            answer: c.text.trim().to_string(),
            author: c.author.clone(),
            answered_at: c.at.clone(),
            decision: String::new(),
        })
        .collect()
}

const STOP: &[&str] = &[
    "the",
    "and",
    "for",
    "per",
    "with",
    "this",
    "that",
    "what",
    "which",
    "are",
    "is",
    "be",
    "to",
    "of",
    "in",
    "on",
    "or",
    "a",
    "an",
    "it",
    "its",
    "as",
    "by",
    "at",
    "we",
    "i",
    "you",
    "do",
    "does",
    "should",
    "would",
    "could",
    "will",
    "can",
    "one",
    "plus",
    "default",
    "proposed",
    "building",
    "now",
    "different",
    "answer",
    "changes",
    "change",
    "use",
    "using",
    "final",
    "pending",
    "product",
    "input",
    "decision",
    "decide",
    "until",
    "awaiting",
    "yes",
    "no",
    "not",
    "just",
    "only",
    "skip",
];

/// The words that carry meaning: lowercase, letters and digits, two or
/// more characters, no filler.
fn words(text: &str) -> HashSet<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .map(|w| w.to_ascii_lowercase())
        .filter(|w| w.len() >= 2 && !STOP.contains(&w.as_str()))
        .collect()
}

/// Whether `a` settles the pending `item`: its question shares at least
/// two meaningful words with the item and half of the shorter's.
pub fn answers_item(item: &str, a: &Answer) -> bool {
    let asked = if a.question.trim().is_empty() {
        &a.answer
    } else {
        &a.question
    };
    // The question's first sentence is what was asked; the rest is the
    // proposal around it.
    let head = asked.split(['?', '\n']).next().unwrap_or(asked).to_string();
    let q = words(&head);
    let i = words(item);
    if q.is_empty() || i.is_empty() {
        return false;
    }
    let shared = q.intersection(&i).count();
    shared >= 2 && shared * 2 >= q.len().min(i.len())
}

/// Whether a heading names items that wait on someone's decision.
pub fn pending_heading(line: &str) -> bool {
    let h = line
        .trim()
        .trim_start_matches('#')
        .trim()
        .trim_matches('*')
        .trim()
        .to_ascii_lowercase();
    if h.is_empty() || h.len() > 80 {
        return false;
    }
    [
        "pending",
        "open question",
        "awaiting",
        "needs product",
        "product input",
        "to decide",
        "undecided",
        "decisions needed",
        "needs a decision",
        "unresolved",
    ]
    .iter()
    .any(|k| h.contains(k))
}

fn is_heading(line: &str) -> bool {
    let t = line.trim();
    t.starts_with('#') || (t.starts_with("**") && t.ends_with("**") && t.len() > 4)
}

fn list_item(line: &str) -> Option<&str> {
    let t = line.trim_start();
    for p in ["- [ ] ", "- [x] ", "- ", "* ", "+ "] {
        if let Some(rest) = t.strip_prefix(p) {
            return Some(rest);
        }
    }
    let digits = t.chars().take_while(|c| c.is_ascii_digit()).count();
    if digits > 0 && t[digits..].starts_with(". ") {
        return Some(&t[digits + 2..]);
    }
    None
}

/// Words that say an answer takes something out or does without it.
const WITHOUT: &[&str] = &[
    "skip",
    "drop",
    "remove",
    "without",
    "don't",
    "do not",
    "no ",
    "omit",
    "leave out",
    "just use",
    "only use",
    "instead",
];

/// The one line on an answer that goes against an acceptance criterion:
/// the answer does without something a criterion asks for (they share two
/// meaningful words). `None` when no criterion is at odds with it.
pub fn conflict(a: &Answer, criteria: &[String]) -> Option<String> {
    let said = a.answer.to_ascii_lowercase();
    if !WITHOUT.iter().any(|w| said.contains(w)) {
        return None;
    }
    let aw = words(&a.answer);
    criteria.iter().find_map(|c| {
        let shared = words(c).intersection(&aw).count();
        (shared >= 2).then(|| {
            format!(
                "The answer on the ticket (\"{}\") goes against the acceptance criterion \"{}\"; \
                 the change follows the answer.",
                crate::truncate_str(first_line(&a.answer), 120),
                crate::truncate_str(c.trim(), 120)
            )
        })
    })
}

fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or(s).trim()
}

/// A description with every answered item taken out of its pending lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settled {
    pub body: String,
    /// The pending items taken out.
    pub removed: Vec<String>,
}

/// Takes every item an answer settles out of the description's pending
/// sections (and a pending heading left with no item), and lists the
/// answers that settled them under [`DECIDED_HEADING`] with, for an answer
/// against an acceptance criterion, the one line that says so.
pub fn settle(body: &str, answers: &[Answer], criteria: &[String]) -> Settled {
    if answers.is_empty() {
        return Settled {
            body: body.to_string(),
            removed: Vec::new(),
        };
    }
    let lines: Vec<&str> = body.lines().collect();
    let mut keep = vec![true; lines.len()];
    let mut removed = Vec::new();
    let mut used: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if !(is_heading(lines[i]) && pending_heading(lines[i])) {
            i += 1;
            continue;
        }
        let head = i;
        let mut items_left = 0;
        let mut taken = 0;
        let mut j = i + 1;
        while j < lines.len() && !is_heading(lines[j]) {
            if let Some(item) = list_item(lines[j]) {
                match answers.iter().position(|a| answers_item(item, a)) {
                    Some(k) => {
                        keep[j] = false;
                        taken += 1;
                        removed.push(item.trim().to_string());
                        if !used.contains(&k) {
                            used.push(k);
                        }
                        // The item's continuation lines go with it.
                        let mut c = j + 1;
                        while c < lines.len()
                            && !lines[c].trim().is_empty()
                            && list_item(lines[c]).is_none()
                            && !is_heading(lines[c])
                            && lines[c].starts_with("  ")
                        {
                            keep[c] = false;
                            c += 1;
                        }
                        j = c;
                        continue;
                    }
                    None => items_left += 1,
                }
            }
            j += 1;
        }
        if items_left == 0 && taken > 0 {
            keep[head] = false;
            // The blank lines and prose of an emptied section go too.
            for (k, line) in lines.iter().enumerate().take(j).skip(head + 1) {
                if list_item(line).is_none() {
                    keep[k] = false;
                }
            }
        }
        i = j;
    }
    if used.is_empty() {
        return Settled {
            body: body.to_string(),
            removed,
        };
    }
    let mut out: Vec<String> = lines
        .iter()
        .zip(&keep)
        .filter(|(_, k)| **k)
        .map(|(l, _)| l.to_string())
        .collect();
    let mut decided: Vec<String> = Vec::new();
    for k in used {
        let a = &answers[k];
        let asked = if a.question.is_empty() {
            String::new()
        } else {
            format!(
                "{}: ",
                crate::truncate_str(a.question.split('?').next().unwrap_or("").trim(), 100)
            )
        };
        let by = if a.author.trim().is_empty() {
            String::new()
        } else {
            format!(" ({})", a.author.trim())
        };
        decided.push(format!(
            "- {asked}\"{}\"{by}",
            crate::truncate_str(first_line(&a.answer), 160)
        ));
        if let Some(c) = conflict(a, criteria) {
            decided.push(format!("- {c}"));
        }
    }
    match out.iter().position(|l| l.trim() == DECIDED_HEADING) {
        Some(at) => {
            let mut at = at + 1;
            for d in decided {
                if !out.contains(&d) {
                    out.insert(at, d);
                    at += 1;
                }
            }
        }
        None => {
            while out.last().is_some_and(|l| l.trim().is_empty()) {
                out.pop();
            }
            out.push(String::new());
            out.push(DECIDED_HEADING.to_string());
            out.extend(decided);
        }
    }
    let mut body_out = out.join("\n");
    if body.ends_with('\n') {
        body_out.push('\n');
    }
    Settled {
        body: body_out,
        removed,
    }
}

/// The ticket key a description names on its "Ticket:" line.
pub fn ticket_of(body: &str) -> Option<String> {
    for line in body.lines() {
        let Some(rest) = line.trim().strip_prefix("Ticket:") else {
            continue;
        };
        let rest = rest.trim().trim_start_matches('[');
        let key: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '#')
            .collect();
        if !key.is_empty() {
            return Some(key.to_ascii_uppercase());
        }
    }
    None
}

/// A ticket's decisions and acceptance criteria, as the worker last read
/// them, for [`settle`] where a description is written.
#[derive(Debug, Clone, Default)]
pub struct Known {
    pub answers: Vec<Answer>,
    pub criteria: Vec<String>,
}

static KNOWN: Mutex<Option<HashMap<String, Known>>> = Mutex::new(None);

/// The most tickets kept in memory at once.
const MAX_KNOWN: usize = 64;

/// The key of a ticket's decisions: the GitHub App installation the run
/// writes with, its repository and the ticket. One team's decisions are
/// read back only by writes made with that team's own installation.
fn known_key(installation: u64, repo: &str, ticket: &str) -> String {
    format!(
        "{installation}:{}@{}",
        repo.to_ascii_lowercase(),
        ticket.to_ascii_uppercase()
    )
}

/// Keeps `ticket`'s decisions for the descriptions written in `repo` with
/// `installation`.
pub fn remember(installation: u64, repo: &str, ticket: &str, known: Known) {
    if ticket.trim().is_empty() {
        return;
    }
    if let Ok(mut m) = KNOWN.lock() {
        let m = m.get_or_insert_with(HashMap::new);
        if m.len() >= MAX_KNOWN {
            m.clear();
        }
        m.insert(known_key(installation, repo, ticket), known);
    }
}

/// What [`remember`] kept for `ticket` in `repo`.
pub fn recall(installation: u64, repo: &str, ticket: &str) -> Option<Known> {
    KNOWN
        .lock()
        .ok()?
        .as_ref()?
        .get(&known_key(installation, repo, ticket))
        .cloned()
}

/// [`settle`] with what is known for the ticket the description names.
pub fn settle_known(installation: u64, repo: &str, body: &str) -> Settled {
    let none = || Settled {
        body: body.to_string(),
        removed: Vec::new(),
    };
    let Some(ticket) = ticket_of(body) else {
        return none();
    };
    let Some(k) = recall(installation, repo, &ticket) else {
        return none();
    };
    settle(body, &k.answers, &k.criteria)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(q: &str, a: &str) -> Answer {
        Answer {
            question: q.into(),
            answer: a.into(),
            author: "a-person".into(),
            ..Default::default()
        }
    }

    #[test]
    fn an_answered_item_leaves_the_pending_list() {
        let body = "## Summary\nAdds the page.\n\n### Decisions pending product input\n- Final heading and intro copy per marina: built with the default for now.\n- Analytics event names\n\n## Testing\n- unit tests\n";
        let a = answer(
            "Final heading and intro copy per marina? Proposed default: '{Marina} Slip Booking'.",
            "Skip the heading and intro copy. Just use the title",
        );
        let s = settle(body, &[a], &[]);
        assert!(
            !s.body.contains("intro copy per marina: built"),
            "{}",
            s.body
        );
        assert!(s.body.contains("Analytics event names"));
        assert!(s.body.contains("### Decisions pending product input"));
        assert!(s.body.contains(DECIDED_HEADING));
        assert_eq!(s.removed.len(), 1);
    }

    #[test]
    fn an_emptied_pending_section_goes() {
        let body = "## Summary\nx\n\n**Open questions**\n- Heading and intro copy for each marina\n\n## Testing\n- ok\n";
        let a = answer(
            "Heading and intro copy per marina?",
            "Skip both, use the title",
        );
        let s = settle(body, &[a], &[]);
        assert!(!s.body.contains("Open questions"), "{}", s.body);
        assert!(s.body.contains("## Testing"));
    }

    #[test]
    fn an_unrelated_answer_changes_nothing() {
        let body = "### Pending\n- Analytics event names\n";
        let s = settle(body, &[answer("Which marina goes first?", "Marina A")], &[]);
        assert_eq!(s.body, body);
    }

    #[test]
    fn an_answer_against_a_criterion_is_said_in_one_line() {
        let a = answer(
            "Heading and intro copy?",
            "Skip the heading and intro copy. Just use the title",
        );
        let c = conflict(
            &a,
            &["Each page shows a heading and intro copy above the portal".into()],
        )
        .unwrap();
        assert!(c.contains("goes against the acceptance criterion"), "{c}");
        assert_eq!(c.lines().count(), 1);
        assert!(conflict(&a, &["The portal loads at phone width".into()]).is_none());
    }

    #[test]
    fn the_ticket_comes_from_the_description() {
        assert_eq!(
            ticket_of("Ticket: [ABC-12](https://x.atlassian.net/browse/ABC-12)\n").as_deref(),
            Some("ABC-12")
        );
        assert_eq!(ticket_of("no ticket").as_deref(), None);
    }

    #[test]
    fn every_answer_and_instruction_counts_whatever_run_handled_it() {
        use crate::ticket_comments::State;
        let c = |id: &str, class: Class, at: &str| Comment {
            ticket_id: "ABC-1".into(),
            comment_id: id.into(),
            author: "a-person".into(),
            text: format!("text {id}"),
            at: at.into(),
            run_id: "an-earlier-run".into(),
            state: State::Handled,
            class: Some(class),
            ..Default::default()
        };
        let got = from_comments(&[
            c("2", Class::Instruction, "2026-01-02"),
            c("1", Class::Answer, "2026-01-01"),
            c("3", Class::NotForCoderhelm, "2026-01-03"),
        ]);
        assert_eq!(
            got.iter().map(|a| a.answer.as_str()).collect::<Vec<_>>(),
            ["text 1", "text 2"]
        );
    }
}
