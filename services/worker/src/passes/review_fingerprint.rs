//! What "nothing changed since my last review" means: a stored fingerprint of
//! everything the verdict rests on, never the commit alone.
//!
//! A review stores it on its record. Before reviewing again the worker reads
//! the same things and compares them. Each part is hashed on its own, so the
//! comparison can say what changed; a part that couldn't be read counts as
//! changed, and a record without a fingerprint counts as changed, so anything
//! unknown is reviewed afresh.

use common::review_description::body_hash;
use serde::{Deserialize, Serialize};

/// Everything a verdict rests on, as read for one review.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fingerprint {
    pub head: String,
    /// The description's meaningful text (`review_description::normalized`).
    pub description: String,
    /// CoderHelm's own threads: which exist and which are resolved.
    pub threads: Option<String>,
    /// CI on the head, the checks failing on the base, and the plan evidence.
    pub results: Option<String>,
    /// What people wrote, a bare "re-review" aside.
    pub people: Option<String>,
}

/// One thing that differs from the stored fingerprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Changed {
    Commit,
    Description,
    Threads,
    Results,
    People,
    /// The earlier review stored no fingerprint.
    NoRecord,
}

impl Changed {
    fn words(self) -> &'static str {
        match self {
            Changed::Commit => "the commit",
            Changed::Description => "the description",
            Changed::Threads => "the review threads",
            Changed::Results => "the CI or plan results",
            Changed::People => "what people wrote",
            Changed::NoRecord => "my earlier review has no record of what it read",
        }
    }
}

/// Fixed words for what changed, for a posted line.
pub fn in_words(changed: &[Changed]) -> String {
    let w: Vec<&str> = changed.iter().map(|c| c.words()).collect();
    match w.as_slice() {
        [] => String::new(),
        [one] => (*one).to_string(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

fn differs(a: &Option<String>, b: &Option<String>) -> bool {
    a.is_none() || b.is_none() || a != b
}

/// What differs between the stored fingerprint and `now`. Empty only when
/// every part was read and is identical.
pub fn changes(stored: Option<&Fingerprint>, now: &Fingerprint) -> Vec<Changed> {
    let Some(s) = stored else {
        return vec![Changed::NoRecord];
    };
    let mut out = Vec::new();
    if s.head != now.head {
        out.push(Changed::Commit);
    }
    if s.description != now.description {
        out.push(Changed::Description);
    }
    if differs(&s.threads, &now.threads) {
        out.push(Changed::Threads);
    }
    if differs(&s.results, &now.results) {
        out.push(Changed::Results);
    }
    if differs(&s.people, &now.people) {
        out.push(Changed::People);
    }
    out
}

/// A comment alone doesn't change what a verdict rests on: a reply's reading
/// may start a formal review only when something other than people's words
/// changed. A thread a person's words settled changes `Threads`.
pub fn more_than_words(changed: &[Changed]) -> bool {
    changed.iter().any(|c| *c != Changed::People)
}

/// The description part.
pub fn description_part(body: &str) -> String {
    body_hash(body)
}

/// The threads part, from (thread id, resolved).
pub fn threads_part<I: IntoIterator<Item = (String, bool)>>(threads: I) -> String {
    let mut lines: Vec<String> = threads
        .into_iter()
        .map(|(id, resolved)| format!("{id}={resolved}"))
        .collect();
    lines.sort();
    body_hash(&lines.join("\n"))
}

/// The results part: the head's check runs (name and result, CoderHelm's own
/// review check aside, which this job itself moves), the checks failing on the
/// base, and the plan evidence.
pub fn results_part(head_runs: &serde_json::Value, base_failing: &[String], plan: &str) -> String {
    let mut lines: Vec<String> = head_runs["check_runs"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
        .iter()
        .filter(|r| r["name"].as_str() != Some(common::review_check::CHECK_NAME))
        .map(|r| {
            format!(
                "{}={}",
                r["name"].as_str().unwrap_or("?"),
                r["conclusion"]
                    .as_str()
                    .unwrap_or_else(|| r["status"].as_str().unwrap_or("?"))
            )
        })
        .collect();
    lines.sort();
    let mut base: Vec<&str> = base_failing.iter().map(String::as_str).collect();
    base.sort_unstable();
    body_hash(&format!(
        "{}\nbase:{}\nplan:{plan}",
        lines.join("\n"),
        base.join(",")
    ))
}

/// The people part, from comments and review bodies (GitHub objects). What a
/// bot wrote and a bare "re-review" aren't new information.
pub fn people_part(items: &[serde_json::Value]) -> String {
    let mut bodies: Vec<String> = items
        .iter()
        .filter(|c| super::review_policy::is_person(&c["user"]))
        .filter_map(|c| c["body"].as_str())
        .map(str::trim)
        .filter(|b| !b.is_empty() && !super::review_policy::is_bare_command(b))
        .map(|b| b.to_string())
        .collect();
    bodies.sort();
    body_hash(&bodies.join("\n--\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(crate) fn fp() -> Fingerprint {
        Fingerprint {
            head: "h1".into(),
            description: description_part("a description"),
            threads: Some(threads_part([("t1".to_string(), false)])),
            results: Some(results_part(
                &json!({"check_runs": [{"name": "build", "conclusion": "success"}]}),
                &[],
                "",
            )),
            people: Some(people_part(&[])),
        }
    }

    #[test]
    fn identical_means_every_part_was_read_and_is_the_same() {
        assert!(changes(Some(&fp()), &fp()).is_empty());
        assert_eq!(changes(None, &fp()), vec![Changed::NoRecord]);
        // A part that couldn't be read isn't the same.
        let mut unread = fp();
        unread.results = None;
        assert_eq!(changes(Some(&fp()), &unread), vec![Changed::Results]);
        assert_eq!(changes(Some(&unread), &unread), vec![Changed::Results]);
    }

    #[test]
    fn each_thing_a_verdict_rests_on_changes_it() {
        let mut new = fp();
        new.head = "h2".into();
        assert_eq!(changes(Some(&fp()), &new), vec![Changed::Commit]);
        let mut new = fp();
        new.description = description_part("a description, confirmed in staging");
        assert_eq!(changes(Some(&fp()), &new), vec![Changed::Description]);
        // Ticking a box or re-wrapping is not an edit.
        new.description = description_part("a  description");
        assert!(changes(Some(&fp()), &new).is_empty());
        let mut new = fp();
        new.threads = Some(threads_part([("t1".to_string(), true)]));
        assert_eq!(changes(Some(&fp()), &new), vec![Changed::Threads]);
        let mut new = fp();
        new.results = Some(results_part(
            &json!({"check_runs": [{"name": "build", "conclusion": "failure"}]}),
            &[],
            "",
        ));
        assert_eq!(changes(Some(&fp()), &new), vec![Changed::Results]);
    }

    #[test]
    fn coderhelms_own_check_and_a_bare_ask_are_not_changes() {
        let with_own = results_part(
            &json!({"check_runs": [
                {"name": "build", "conclusion": "success"},
                {"name": common::review_check::CHECK_NAME, "status": "in_progress"}
            ]}),
            &[],
            "",
        );
        assert_eq!(Some(with_own), fp().results);
        let person = json!({"login": "dev", "type": "User"});
        let bot = json!({"login": "coderhelm[bot]", "type": "Bot"});
        assert_eq!(
            people_part(&[
                json!({"user": person, "body": "@coderhelm re-review please"}),
                json!({"user": bot, "body": "Answered."}),
            ]),
            people_part(&[])
        );
        assert_ne!(
            people_part(&[json!({"user": person, "body": "The deploy note is the blocker."})]),
            people_part(&[])
        );
    }

    #[test]
    fn a_comment_alone_is_only_words() {
        assert!(!more_than_words(&[]));
        assert!(!more_than_words(&[Changed::People]));
        assert!(more_than_words(&[Changed::People, Changed::Threads]));
        assert!(more_than_words(&[Changed::NoRecord]));
        assert!(more_than_words(&[Changed::Description]));
        assert_eq!(
            in_words(&[Changed::Description, Changed::Threads, Changed::People]),
            "the description, the review threads and what people wrote"
        );
    }
}
