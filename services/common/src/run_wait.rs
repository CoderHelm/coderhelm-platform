//! What a run waiting on a person (`needs_input`) is waiting for.
//!
//! A run did its work, opened a draft pull request with a check's finding as
//! its open item, and was set to `needs_input` with no question: nothing
//! asked anyone anything, the run page said it needed "more detail", and the
//! run sat for days while its pull request only needed a review. Waiting on
//! a person is now always for something named:
//!
//! - every `needs_input` write carries a [`WaitingOn`] (`waiting_on`): the
//!   questions to answer on the run page, or one named action (review pull
//!   request #n, answer an approval, add the missing detail to the ticket),
//!   with where to do it. The only way to write the status is
//!   [`NeedsInput::needs_input`], which takes one; a test reads the
//!   services' sources so no write sets the status any other way;
//! - the run page shows it and the `run.needs_input` event carries it;
//! - a run still waiting [`REMIND_AFTER_HOURS`] after it started waiting
//!   gets one reminder through the same event (the stuck-run sweep);
//! - the stuck-run sweep reports and settles any `needs_input` run with
//!   neither (written before this, or by hand).

use std::collections::HashMap;

use aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};

use crate::handback::HandBack;
use crate::run_questions::{Asked, Question, FIELD_QUESTIONS};
use crate::run_status::NEEDS_INPUT;

/// Run attribute holding the [`WaitingOn`] JSON.
pub const FIELD_WAITING_ON: &str = "waiting_on";
/// Run attribute: when the one reminder for the current wait was sent.
pub const FIELD_REMINDED_AT: &str = "wait_reminded_at";
/// A run waiting on a person this long gets one reminder. Mirrored in
/// lambda/stuck-run-cleanup/handler.py (a test here reads that line).
pub const REMIND_AFTER_HOURS: i64 = 24;

const MAX_ACTION_CHARS: usize = 200;
const MAX_DETAIL_CHARS: usize = 1_500;

/// The one thing a waiting run needs from a person.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingOn {
    /// What to do, in plain words ("Review pull request #12").
    pub action: String,
    /// Why, or what is open, in plain words. May be empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
    /// Where to do it (the pull request, the ticket), when known.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub url: String,
    /// How many questions the run page asks, when the action is to answer
    /// them.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub questions: usize,
    /// When the wait started (RFC 3339): the reminder counts from here.
    #[serde(default)]
    pub since: String,
    /// Why only a person can do it: a code on the human-only list
    /// (`handback::HumanOnly::code`), set by [`NeedsInput::needs_input`].
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub handback: String,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

fn one_line(s: &str, max: usize) -> String {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    crate::truncate_str(&s, max).to_string()
}

fn bounded(s: &str, max: usize) -> String {
    crate::truncate_str(s.trim(), max).to_string()
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

impl WaitingOn {
    /// A named action. `None` when `action` is blank: a wait is never for
    /// nothing.
    pub fn action(action: &str, detail: &str, url: &str) -> Option<Self> {
        let action = one_line(action, MAX_ACTION_CHARS);
        (!action.is_empty()).then(|| WaitingOn {
            action,
            detail: bounded(detail, MAX_DETAIL_CHARS),
            url: url.trim().to_string(),
            questions: 0,
            since: now(),
            handback: String::new(),
        })
    }

    /// A named action written in the code (never blank).
    pub fn fixed(action: &'static str, detail: &str, url: &str) -> Self {
        debug_assert!(!action.trim().is_empty());
        WaitingOn {
            action: action.to_string(),
            detail: bounded(detail, MAX_DETAIL_CHARS),
            url: url.trim().to_string(),
            questions: 0,
            since: now(),
            handback: String::new(),
        }
    }

    /// The questions when there are any, else `fallback`.
    pub fn answer_or(questions: &[Question], where_asked: Option<&str>, fallback: Self) -> Self {
        Self::answer(questions, where_asked).unwrap_or(fallback)
    }

    /// Answer the questions on the run page. `None` when there are none.
    pub fn answer(questions: &[Question], where_asked: Option<&str>) -> Option<Self> {
        let asked: Vec<&Question> = questions
            .iter()
            .filter(|q| !q.question.trim().is_empty())
            .collect();
        if asked.is_empty() {
            return None;
        }
        let action = if asked.len() == 1 {
            "Answer CoderHelm's question on the run page".to_string()
        } else {
            format!(
                "Answer CoderHelm's {} questions on the run page",
                asked.len()
            )
        };
        let mut w = Self::action(&action, &asked[0].question, where_asked.unwrap_or(""))?;
        w.questions = asked.len();
        Some(w)
    }

    /// Review the run's pull request: the run did its work and is waiting
    /// for review, with what is still open on the pull request. `None` when
    /// there is no pull request to point at.
    pub fn review_pr(number: u64, url: &str, open: &str) -> Option<Self> {
        if number == 0 && url.trim().is_empty() {
            return None;
        }
        let action = if number > 0 {
            format!("Review pull request #{number}")
        } else {
            "Review the pull request".to_string()
        };
        let detail = if open.trim().is_empty() {
            "The work is on the pull request. Review it, push a fix or comment on it, and \
             CoderHelm picks it up from there."
                .to_string()
        } else {
            format!(
                "Open on the pull request: {}. Mark it ready once that is dealt with, push a fix \
                 or comment on it, and CoderHelm picks it up from there.",
                one_line(open, MAX_DETAIL_CHARS - 160).trim_end_matches('.')
            )
        };
        Self::action(&action, &detail, url)
    }

    /// The wait in one line, for a notification or a list.
    pub fn line(&self) -> String {
        let mut s = self.action.clone();
        if !self.detail.is_empty() {
            s.push_str(": ");
            s.push_str(&one_line(&self.detail, 600));
        }
        s
    }

    /// The attribute value it is stored as.
    pub fn to_attr(&self) -> AttributeValue {
        AttributeValue::S(serde_json::to_string(self).unwrap_or_default())
    }

    /// Read back from a run record. `None` when absent, unreadable or blank.
    pub fn of_item(item: &HashMap<String, AttributeValue>) -> Option<Self> {
        let w: WaitingOn = item
            .get(FIELD_WAITING_ON)
            .and_then(|v| v.as_s().ok())
            .and_then(|s| serde_json::from_str(s).ok())?;
        (!w.action.trim().is_empty()).then_some(w)
    }
}

/// Whether a run record names what it waits for: questions to answer, or a
/// named action. A `needs_input` run without either is stalled.
pub fn asks_something(item: &HashMap<String, AttributeValue>) -> bool {
    let asked = item
        .get(FIELD_QUESTIONS)
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str::<Asked>(s).ok())
        .is_some_and(|a| a.questions.iter().any(|q| !q.question.trim().is_empty()));
    asked || WaitingOn::of_item(item).is_some()
}

/// The one way to write `needs_input`: with what the run waits for, and
/// why only a person can do it ([`HandBack`]: a reason on the human-only
/// list). A stall, a red check, a budget or an error has no `HandBack`, so
/// it can't end a run this way.
pub trait NeedsInput {
    /// Sets `:s` to `needs_input`, `:sri` to its `status_run_id` and `:wo`
    /// to `wait` with the hand-back's reason. The update expression names
    /// them: `#s = :s, status_run_id = :sri, waiting_on = :wo`.
    fn needs_input(self, run_id: &str, wait: &WaitingOn, why: &HandBack) -> Self;
}

impl NeedsInput for UpdateItemFluentBuilder {
    fn needs_input(self, run_id: &str, wait: &WaitingOn, why: &HandBack) -> Self {
        let mut wait = wait.clone();
        wait.handback = why.why().code().to_string();
        if wait.detail.trim().is_empty() {
            wait.detail = bounded(why.need(), MAX_DETAIL_CHARS);
        }
        self.expression_attribute_values(":s", AttributeValue::S(NEEDS_INPUT.into()))
            .expression_attribute_values(
                ":sri",
                AttributeValue::S(format!("{NEEDS_INPUT}#{run_id}")),
            )
            .expression_attribute_values(":wo", wait.to_attr())
    }
}

/// Whether a run waiting since `since` is due its one reminder at `now`:
/// waiting [`REMIND_AFTER_HOURS`] or more, and not reminded since it started
/// waiting.
pub fn reminder_due(
    since: &str,
    reminded_at: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let parse = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|t| t.with_timezone(&chrono::Utc))
    };
    let Some(since) = parse(since) else {
        return false;
    };
    if now - since < chrono::Duration::hours(REMIND_AFTER_HOURS) {
        return false;
    }
    !reminded_at.and_then(parse).is_some_and(|r| r >= since)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(pairs: &[(&str, &str)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
            .collect()
    }

    #[test]
    fn a_wait_is_never_for_nothing() {
        assert_eq!(WaitingOn::action("  ", "detail", ""), None);
        assert_eq!(WaitingOn::answer(&[], None), None);
        let blank = Question {
            id: "q1".into(),
            question: " ".into(),
            ..Default::default()
        };
        assert_eq!(WaitingOn::answer(&[blank], None), None);
        assert_eq!(WaitingOn::review_pr(0, " ", "x"), None);
    }

    #[test]
    fn a_stopped_run_with_a_pull_request_waits_for_its_review() {
        let w = WaitingOn::review_pr(
            12,
            "https://github.com/o/r/pull/12",
            "The new-feature rules: new items aren't registered",
        )
        .unwrap();
        assert_eq!(w.action, "Review pull request #12");
        assert!(w.detail.contains("new items aren't registered"));
        assert_eq!(w.url, "https://github.com/o/r/pull/12");
        assert!(!w.since.is_empty());
        let back = WaitingOn::of_item(&item(&[(
            FIELD_WAITING_ON,
            &serde_json::to_string(&w).unwrap(),
        )]))
        .unwrap();
        assert_eq!(back, w);
        assert!(w
            .line()
            .starts_with("Review pull request #12: Open on the pull request"));
    }

    #[test]
    fn questions_wait_for_answers() {
        let q = Question {
            id: "q1".into(),
            question: "Per key or per team?".into(),
            ..Default::default()
        };
        let w = WaitingOn::answer(&[q], Some("https://x/1")).unwrap();
        assert_eq!(w.questions, 1);
        assert!(w.action.contains("question"));
    }

    #[test]
    fn a_record_with_neither_asks_nothing() {
        assert!(!asks_something(&item(&[("status", "needs_input")])));
        assert!(!asks_something(&item(&[(
            FIELD_QUESTIONS,
            r#"{"questions":[],"asked_at":""}"#
        )])));
        assert!(!asks_something(&item(&[(
            FIELD_WAITING_ON,
            r#"{"action":" "}"#
        )])));
        assert!(asks_something(&item(&[(
            FIELD_QUESTIONS,
            r#"{"questions":[{"id":"q1","question":"Which?"}],"asked_at":""}"#
        )])));
        assert!(asks_something(&item(&[(
            FIELD_WAITING_ON,
            r#"{"action":"Review pull request #3","since":""}"#
        )])));
    }

    #[test]
    fn one_reminder_after_the_wait() {
        let t = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .with_timezone(&chrono::Utc)
        };
        let since = "2026-11-02T10:00:00Z";
        assert!(!reminder_due(since, None, t("2026-11-03T09:59:00Z")));
        assert!(reminder_due(since, None, t("2026-11-03T10:00:00Z")));
        assert!(!reminder_due(
            since,
            Some("2026-11-03T10:05:00Z"),
            t("2026-11-05T10:00:00Z")
        ));
        // A reminder for an earlier wait doesn't count for this one.
        assert!(reminder_due(
            since,
            Some("2026-11-01T10:00:00Z"),
            t("2026-11-03T11:00:00Z")
        ));
        assert!(!reminder_due("", None, t("2026-11-03T11:00:00Z")));
    }

    #[test]
    fn the_sweep_reminds_after_the_same_time() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let py = std::fs::read_to_string(root.join("lambda/stuck-run-cleanup/handler.py")).unwrap();
        assert!(
            py.contains(&format!("WAIT_REMIND_AFTER_HOURS = {REMIND_AFTER_HOURS}\n")),
            "handler.py must mirror REMIND_AFTER_HOURS"
        );
        assert!(py.contains(&format!("WAITING_ON_FIELD = \"{FIELD_WAITING_ON}\"")));
        assert!(py.contains(&format!("REMINDED_FIELD = \"{FIELD_REMINDED_AT}\"")));
    }

    fn sources() -> Vec<(std::path::PathBuf, String)> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .canonicalize()
            .unwrap();
        let mut out = Vec::new();
        let mut stack = vec![root.join("worker/src"), root.join("gateway/src")];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push((p.clone(), std::fs::read_to_string(&p).unwrap_or_default()));
                }
            }
        }
        out
    }

    /// The code before the file's tests.
    fn non_test(text: &str) -> &str {
        text.find("#[cfg(test)]")
            .map(|i| &text[..i])
            .unwrap_or(text)
    }

    #[test]
    fn no_write_sets_needs_input_without_what_it_waits_for() {
        let mut bad = Vec::new();
        for (path, text) in sources() {
            let code = non_test(&text);
            let squashed: String = code.split_whitespace().collect();
            for needle in [
                "attr_s(\"needs_input\")",
                "attr_s(NEEDS_INPUT)",
                "attr_s(run_status::NEEDS_INPUT)",
                "attr_s(common::run_status::NEEDS_INPUT)",
                "AttributeValue::S(\"needs_input\".into())",
                "attr_s(&format!(\"needs_input#",
            ] {
                if squashed.contains(needle) {
                    bad.push(format!("{}: {needle}", path.display()));
                }
            }
            if code.contains(".needs_input(") && !code.contains("waiting_on = :wo") {
                bad.push(format!(
                    "{}: calls .needs_input( without `waiting_on = :wo` in its update",
                    path.display()
                ));
            }
        }
        assert!(
            bad.is_empty(),
            "write needs_input only with run_wait::NeedsInput::needs_input:\n{}",
            bad.join("\n")
        );
    }
}
