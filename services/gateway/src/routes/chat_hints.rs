//! What a Chat question names, read without a model: a ticket key, a pull
//! request number, a CVE or advisory id, a quoted title, a status question
//! ("what's the latest", "where are we") or "what's running". The hints put
//! the tool that holds the answer first and tell the model where to look;
//! they never route on their own (the model still picks its tools).
//!
//! The same reading tells the answer check (`chat_agent`) when an answer
//! that says it found nothing never looked where the answer would be.

use std::sync::LazyLock;

use regex::Regex;

static TICKET_KEY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b([A-Z][A-Z0-9]{1,9}-[0-9]{1,7})\b").unwrap());
static CVE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(CVE-[0-9]{4}-[0-9]{4,7})\b").unwrap());
static GHSA: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(GHSA-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4})\b").unwrap());
static PR_NUMBER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:\bpull request|\bPR|\bpull|\bmerge request|\bMR)\s*(?:number\s*)?#?\s*([0-9]{1,7})\b|(?:^|[\s(])#([0-9]{1,7})\b").unwrap()
});
static QUOTED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new("\"([^\"]{4,200})\"|\u{201c}([^\u{201d}]{4,200})\u{201d}|\u{2018}([^\u{2019}]{4,200})\u{2019}|`([^`]{4,200})`")
        .unwrap()
});

/// Words that ask where something stands.
const STATUS_WORDS: &[&str] = &[
    "latest",
    "status",
    "where are we",
    "progress",
    "update on",
    "updates on",
    "what happened",
    "what's happening",
    "going on with",
    "stuck",
    "why did",
    "why was",
    "done yet",
    "is it done",
    "finished",
    "shipped",
    "merged",
    "is it fixed",
    "fixed in",
    "been fixed",
    "blocked",
    "who is working",
    "who's working",
    "eta",
    "state of",
];

/// Words that ask what is running right now.
const RUNNING_WORDS: &[&str] = &[
    "running now",
    "running right now",
    "what's running",
    "whats running",
    "what is running",
    "in flight",
    "in progress",
    "working on right now",
    "working on now",
    "currently running",
    "active runs",
    "runs right now",
];

/// Words that ask about security or infrastructure findings.
const FINDING_WORDS: &[&str] = &[
    "cve",
    "vulnerab",
    "advisory",
    "advisories",
    "security finding",
    "dependency finding",
    "dependabot",
    "infra finding",
    "infrastructure finding",
    "misconfig",
    "quarantined",
    "flaky test",
    "security issue",
];

/// Prefixes of ids that look like ticket keys but are not.
const NOT_TICKETS: &[&str] = &[
    "CVE", "GHSA", "UTF", "SHA", "ISO", "RFC", "HTTP", "TLS", "SSL", "AES", "MD", "ES", "IPV",
    "COVID", "OWASP", "CWE", "PEP",
];

/// What a question names.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Hints {
    pub tickets: Vec<String>,
    pub prs: Vec<u64>,
    /// CVE and GHSA ids, upper case.
    pub advisories: Vec<String>,
    pub quoted: Vec<String>,
    /// It asks where something stands.
    pub status: bool,
    /// It asks what is running now.
    pub running: bool,
    /// It asks about security or infrastructure findings.
    pub findings: bool,
}

/// The most of each kind a question carries into the note.
const MAX_EACH: usize = 5;

fn push_unique<T: PartialEq>(v: &mut Vec<T>, x: T) {
    if v.len() < MAX_EACH && !v.contains(&x) {
        v.push(x);
    }
}

pub fn read(question: &str) -> Hints {
    let q = question.trim();
    let lower = q.to_lowercase();
    let mut h = Hints::default();
    for c in CVE.captures_iter(q) {
        push_unique(&mut h.advisories, c[1].to_ascii_uppercase());
    }
    for c in GHSA.captures_iter(q) {
        push_unique(&mut h.advisories, c[1].to_ascii_uppercase());
    }
    for c in TICKET_KEY.captures_iter(q) {
        let key = c[1].to_string();
        // A CVE or advisory id is not a ticket, nor is a standard's name.
        let prefix = key.split('-').next().unwrap_or("");
        if NOT_TICKETS.contains(&prefix) {
            continue;
        }
        push_unique(&mut h.tickets, key);
    }
    for c in PR_NUMBER.captures_iter(q) {
        if let Some(n) = c
            .get(1)
            .or_else(|| c.get(2))
            .and_then(|m| m.as_str().parse::<u64>().ok())
        {
            push_unique(&mut h.prs, n);
        }
    }
    for c in QUOTED.captures_iter(q) {
        if let Some(m) = (1..=4).find_map(|i| c.get(i)) {
            let t = m.as_str().trim();
            // A quoted word or path is not a title.
            if t.split_whitespace().count() >= 2 && !t.contains('/') {
                push_unique(&mut h.quoted, t.to_string());
            }
        }
    }
    h.running = RUNNING_WORDS.iter().any(|w| lower.contains(w));
    h.findings = !h.advisories.is_empty() || FINDING_WORDS.iter().any(|w| lower.contains(w));
    h.status = h.running || STATUS_WORDS.iter().any(|w| has_word(&lower, w));
    h
}

/// `w` in `text` at word boundaries ("eta" not in "metadata").
fn has_word(text: &str, w: &str) -> bool {
    text.match_indices(w).any(|(i, _)| {
        let before = text[..i].chars().next_back();
        let after = text[i + w.len()..].chars().next();
        !before.is_some_and(|c| c.is_alphanumeric()) && !after.is_some_and(|c| c.is_alphanumeric())
    })
}

impl Hints {
    /// The question is about the team's own work: a ticket, a pull request,
    /// a run, a finding, a quoted title, or where something stands.
    pub fn about_work(&self) -> bool {
        self.status
            || self.running
            || self.findings
            || !self.tickets.is_empty()
            || !self.prs.is_empty()
            || !self.quoted.is_empty()
    }
}

/// An answer that says it found nothing ("I couldn't find anything on
/// that", "there's no record of", "I can't give you a status").
pub fn says_nothing_found(answer: &str) -> bool {
    const PHRASES: &[&str] = &[
        "couldn't find",
        "could not find",
        "can't find",
        "cannot find",
        "didn't find",
        "did not find",
        "found nothing",
        "nothing on that",
        "nothing about",
        "no record",
        "no mention",
        "no information",
        "don't have any information",
        "don't see any",
        "not able to find",
        "unable to find",
        "can't give you a status",
        "cannot give you a status",
        "no results",
        "isn't in the",
        "aren't in the",
        "not in the team's",
    ];
    let a = answer.to_lowercase().replace('\u{2019}', "'");
    PHRASES.iter().any(|p| a.contains(p))
}

/// An answer that says it guessed a field, column or type name instead of
/// reading the schema.
pub fn says_it_guessed(answer: &str) -> bool {
    let a = answer.to_lowercase();
    a.split(['.', '\n', ';']).any(|s| {
        (s.contains("guess") || s.contains("assumed")) && {
            s.contains("field")
                || s.contains("column")
                || s.contains("schema")
                || s.contains("property")
                || s.contains("attribute")
                || s.contains("type name")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quoted_title_and_the_word_latest_make_a_status_question() {
        let h = read(
            "What's the latest on \u{201c}Embed the booking portal on the signup pages\u{201d}?",
        );
        assert_eq!(h.quoted, ["Embed the booking portal on the signup pages"]);
        assert!(h.status && h.about_work());
        assert!(h.tickets.is_empty() && h.prs.is_empty());
        let h = read("what's the latest on \"Fix the login redirect\"");
        assert_eq!(h.quoted, ["Fix the login redirect"]);
    }

    #[test]
    fn keys_numbers_and_ids_are_read() {
        let h = read("Where are we on ABC-123 and PR #45? Also see pull request 7 and #9.");
        assert_eq!(h.tickets, ["ABC-123"]);
        assert_eq!(h.prs, [45, 7, 9]);
        let h = read("Is CVE-2024-12345 fixed in shop-web? And ghsa-abcd-1234-wxyz?");
        assert_eq!(h.advisories, ["CVE-2024-12345", "GHSA-ABCD-1234-WXYZ"]);
        assert!(h.tickets.is_empty(), "{h:?}");
        assert!(h.findings && h.status);
        assert!(read("Do we decode UTF-8 and hash with SHA-256?")
            .tickets
            .is_empty());
    }

    #[test]
    fn what_is_running_is_its_own_question() {
        let h = read("What's running now?");
        assert!(h.running && h.status && h.about_work());
    }

    #[test]
    fn a_code_question_is_not_about_work() {
        for q in [
            "How is harbor-api put together?",
            "Where are refunds handled?",
            "What does src/jobs/expireHolds.ts do?",
            "Explain the metadata loader in `src/meta.ts`",
        ] {
            let h = read(q);
            assert!(!h.about_work(), "{q}: {h:?}");
        }
    }

    #[test]
    fn nothing_found_and_guessing_are_read_from_the_answer() {
        assert!(says_nothing_found(
            "I couldn\u{2019}t find anything on that in the team\u{2019}s docs, so I can\u{2019}t give you a status."
        ));
        assert!(!says_nothing_found("The ticket is in review [S1]."));
        assert!(says_it_guessed("I looked under a field name I guessed."));
        assert!(!says_it_guessed("The page field is `slug` [S2]."));
    }
}
