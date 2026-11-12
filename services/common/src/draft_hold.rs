//! Why a finished run's pull request is still a draft, on the run record.
//!
//! A run once finished with CI green and CoderHelm's review approving, and
//! its pull request stayed a draft: CoderHelm's own sandbox checks had
//! failed on that head, the mark-ready step returned without a word, and the
//! run page said "completed". Nobody was told why the pull request wasn't
//! ready. Other mark-ready messages failed on GitHub's rate limit and were
//! dropped, with the same silent result.
//!
//! The rules:
//!
//! - every time CoderHelm decides a finished run's pull request stays a
//!   draft, the reasons go on the run as a [`DraftHold`] (`draft_hold`),
//!   each with what a person can do about it;
//! - a hold a person must act on ([`Kind::Held`], [`Kind::Failed`]) also
//!   sets the run to `needs_input`, with a [`crate::run_wait::WaitingOn`]
//!   that names the pull request and the reasons, so the run never reads
//!   as plain "completed" while its pull request is held;
//! - a wait CoderHelm settles by itself ([`Kind::Waiting`]) leaves the
//!   status alone and says what it waits for;
//! - once the pull request is ready (or closed), the hold is removed and
//!   `draft_settled` says how it ended, so the sweep stops looking at it.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};

/// Run attribute holding the [`DraftHold`] JSON.
pub const FIELD: &str = "draft_hold";
/// Run attribute set once the run's pull request left draft or was closed
/// (`ready`, `closed`): nothing holds it any more.
pub const FIELD_SETTLED: &str = "draft_settled";

const MAX_WHAT: usize = 600;
const MAX_TODO: usize = 400;
const MAX_REASONS: usize = 8;

/// What kind of hold it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Something a person must decide or fix keeps it a draft.
    #[default]
    Held,
    /// CoderHelm waits for something that settles by itself (a repeat run,
    /// checks still running) and checks again on its own.
    Waiting,
    /// CoderHelm decided it was ready but couldn't mark it: GitHub kept
    /// refusing through every retry.
    Failed,
}

/// One reason, with what a person can do about it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reason {
    /// What holds it, in plain words.
    pub what: String,
    /// What to do about it, in plain words.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub todo: String,
}

fn one_line(s: &str, max: usize) -> String {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    crate::truncate_str(&s, max).to_string()
}

impl Reason {
    pub fn new(what: impl AsRef<str>, todo: impl AsRef<str>) -> Self {
        Reason {
            what: one_line(what.as_ref(), MAX_WHAT),
            todo: one_line(todo.as_ref(), MAX_TODO),
        }
    }

    /// `what. todo`, one sentence each.
    pub fn line(&self) -> String {
        let what = self.what.trim().trim_end_matches('.');
        if self.todo.is_empty() {
            format!("{what}.")
        } else {
            format!("{what}. {}", self.todo.trim())
        }
    }
}

/// Why a finished run's pull request is still a draft.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DraftHold {
    pub kind: Kind,
    /// Never empty: a hold always names its reason.
    pub reasons: Vec<Reason>,
    #[serde(default)]
    pub pr_number: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pr_url: String,
    /// When the pull request was first held for these reasons (RFC 3339).
    #[serde(default)]
    pub since: String,
    /// When CoderHelm last decided (RFC 3339): the sweep decides again once
    /// this is old.
    #[serde(default)]
    pub checked_at: String,
    /// The head commit it was decided on: a wait is bounded per head
    /// ([`DraftHold::stalled`]).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub head: String,
    /// CoderHelm marked the pull request ready so the checks that skip
    /// drafts run, and it is still being proven (or failed its proof).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ready: bool,
}

/// A wait whose reason hasn't changed on the same head for this long is a
/// hold that names what didn't happen.
pub const WAIT_LIMIT_SECS: i64 = 2 * 3600;

impl DraftHold {
    /// A hold, or `None` when there is no reason: a hold is never for
    /// nothing.
    pub fn new(kind: Kind, reasons: Vec<Reason>, pr_number: u64, pr_url: &str) -> Option<Self> {
        let mut reasons: Vec<Reason> = reasons
            .into_iter()
            .filter(|r| !r.what.trim().is_empty())
            .collect();
        reasons.dedup();
        reasons.truncate(MAX_REASONS);
        if reasons.is_empty() {
            return None;
        }
        let now = chrono::Utc::now().to_rfc3339();
        Some(DraftHold {
            kind,
            reasons,
            pr_number,
            pr_url: pr_url.trim().to_string(),
            since: now.clone(),
            checked_at: now,
            head: String::new(),
            ready: false,
        })
    }

    /// Whether the wait `why` on `head` has stalled at `now`: this record
    /// waited on the same reason on the same head for [`WAIT_LIMIT_SECS`]
    /// or more, or is already the hold that said so (its reason starts
    /// with the wait's). A different head or reason starts over.
    pub fn stalled(&self, why: &str, head: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
        if head.is_empty() || self.head != head {
            return false;
        }
        let why = Reason::new(why, "").what;
        let why = why.trim_end_matches('.');
        if why.is_empty() {
            return false;
        }
        match self.kind {
            Kind::Waiting => {
                self.reasons
                    .first()
                    .is_some_and(|r| r.what.trim_end_matches('.') == why)
                    && chrono::DateTime::parse_from_rfc3339(&self.since).is_ok_and(|t| {
                        (now - t.with_timezone(&chrono::Utc)).num_seconds() >= WAIT_LIMIT_SECS
                    })
            }
            Kind::Held => self.reasons.iter().any(|r| r.what.starts_with(why)),
            Kind::Failed => false,
        }
    }

    /// Keeps the earlier `since` when the reasons are the same ones.
    pub fn continuing(mut self, before: Option<&DraftHold>) -> Self {
        if let Some(b) = before {
            if b.kind == self.kind && b.reasons == self.reasons && !b.since.is_empty() {
                self.since = b.since.clone();
            }
        }
        self
    }

    /// Whether a person must act (the run waits on them).
    pub fn needs_person(&self) -> bool {
        matches!(self.kind, Kind::Held | Kind::Failed)
    }

    fn pr_words(&self) -> String {
        if self.pr_number > 0 {
            format!("pull request #{}", self.pr_number)
        } else {
            "the pull request".to_string()
        }
    }

    /// What the run page says, in one line: "Finished, but the PR stays a
    /// draft: <reason>. <what to do>".
    pub fn summary(&self) -> String {
        let reasons = self
            .reasons
            .iter()
            .map(Reason::line)
            .collect::<Vec<_>>()
            .join(" ");
        match self.kind {
            Kind::Held if self.ready => format!(
                "Finished; {} was marked ready so its checks run, and is held on: {reasons}",
                self.pr_words()
            ),
            Kind::Waiting if self.ready => format!(
                "Finished; {} was marked ready so its checks run, and is still being proven: \
                 {reasons} CoderHelm checks again by itself.",
                self.pr_words()
            ),
            Kind::Held => format!("Finished, but {} stays a draft: {reasons}", self.pr_words()),
            Kind::Waiting => format!(
                "Finished; {} stays a draft for now: {reasons} CoderHelm checks again by itself \
                 and marks it ready when that is done.",
                self.pr_words()
            ),
            Kind::Failed => format!(
                "Finished, and {} should be ready, but CoderHelm couldn't mark it: {reasons}",
                self.pr_words()
            ),
        }
    }

    /// What a held run waits for from a person.
    pub fn waiting_on(&self) -> crate::run_wait::WaitingOn {
        let detail = self
            .reasons
            .iter()
            .map(Reason::line)
            .collect::<Vec<_>>()
            .join("\n");
        let action = if self.pr_number > 0 {
            format!(
                "Deal with what keeps pull request #{} a draft",
                self.pr_number
            )
        } else {
            "Deal with what keeps the pull request a draft".to_string()
        };
        crate::run_wait::WaitingOn::action(&action, &detail, &self.pr_url).unwrap_or_else(|| {
            crate::run_wait::WaitingOn::fixed(
                "Deal with what keeps the pull request a draft",
                &detail,
                &self.pr_url,
            )
        })
    }

    pub fn to_attr(&self) -> AttributeValue {
        AttributeValue::S(serde_json::to_string(self).unwrap_or_default())
    }

    /// Read back from a run record. `None` when absent, unreadable or
    /// without a reason.
    pub fn of_item(item: &HashMap<String, AttributeValue>) -> Option<Self> {
        let h: DraftHold = item
            .get(FIELD)
            .and_then(|v| v.as_s().ok())
            .and_then(|s| serde_json::from_str(s).ok())?;
        (!h.reasons.is_empty()).then_some(h)
    }

    /// Whether the last decision is older than `secs` at `now`.
    pub fn stale(&self, secs: i64, now: chrono::DateTime<chrono::Utc>) -> bool {
        chrono::DateTime::parse_from_rfc3339(&self.checked_at)
            .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds() >= secs)
            .unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hold_always_names_its_reason() {
        assert_eq!(DraftHold::new(Kind::Held, vec![], 5, ""), None);
        assert_eq!(
            DraftHold::new(Kind::Held, vec![Reason::new("  ", "x")], 5, ""),
            None
        );
        let h = DraftHold::new(
            Kind::Held,
            vec![Reason::new(
                "Security items are open in the description",
                "Fix them on this branch, or delete each line once it is checked.",
            )],
            583,
            "https://github.com/acme/api/pull/583",
        )
        .unwrap();
        assert_eq!(
            h.summary(),
            "Finished, but pull request #583 stays a draft: Security items are open in the \
             description. Fix them on this branch, or delete each line once it is checked."
        );
        assert!(h.needs_person());
        let w = h.waiting_on();
        assert_eq!(w.action, "Deal with what keeps pull request #583 a draft");
        assert!(w.detail.contains("Security items"));
        assert_eq!(w.url, "https://github.com/acme/api/pull/583");
    }

    #[test]
    fn a_wait_says_coderhelm_goes_on_by_itself() {
        let h = DraftHold::new(
            Kind::Waiting,
            vec![Reason::new(
                "The repeat run of the affected test hasn't passed yet",
                "",
            )],
            9,
            "",
        )
        .unwrap();
        assert!(!h.needs_person());
        assert!(h.summary().contains("CoderHelm checks again by itself"));
    }

    #[test]
    fn it_round_trips_and_keeps_its_start_while_the_reasons_stay() {
        let r = vec![Reason::new("a", "b")];
        let mut first = DraftHold::new(Kind::Held, r.clone(), 1, "").unwrap();
        first.since = "2026-11-06T22:17:00Z".into();
        let item: HashMap<String, AttributeValue> =
            [(FIELD.to_string(), first.to_attr())].into_iter().collect();
        assert_eq!(DraftHold::of_item(&item), Some(first.clone()));
        let again = DraftHold::new(Kind::Held, r, 1, "")
            .unwrap()
            .continuing(Some(&first));
        assert_eq!(again.since, "2026-11-06T22:17:00Z");
        let other = DraftHold::new(Kind::Held, vec![Reason::new("c", "")], 1, "")
            .unwrap()
            .continuing(Some(&first));
        assert_ne!(other.since, "2026-11-06T22:17:00Z");
    }

    #[test]
    fn a_wait_stalls_after_two_hours_on_the_same_head() {
        let t = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .with_timezone(&chrono::Utc)
        };
        let why = "this failing-test fix waits: checks are skipped";
        let mut w = DraftHold::new(Kind::Waiting, vec![Reason::new(why, "")], 1, "").unwrap();
        w.since = "2026-11-10T06:35:00Z".into();
        w.head = "abc".into();
        assert!(!w.stalled(why, "abc", t("2026-11-10T08:34:00Z")));
        assert!(w.stalled(why, "abc", t("2026-11-10T08:35:00Z")));
        assert!(!w.stalled(why, "def", t("2026-11-10T12:00:00Z")));
        assert!(!w.stalled("another reason", "abc", t("2026-11-10T12:00:00Z")));
        // Already the hold that said so: it stays one on that head.
        let mut h = DraftHold::new(
            Kind::Held,
            vec![Reason::new(
                format!("{why}, and that hasn't changed in 2 hours"),
                "x",
            )],
            1,
            "",
        )
        .unwrap();
        h.head = "abc".into();
        assert!(h.stalled(why, "abc", t("2026-11-10T08:36:00Z")));
        assert!(!h.stalled(why, "def", t("2026-11-10T08:36:00Z")));
        h.ready = true;
        assert!(h.summary().contains("was marked ready so its checks run"));
    }

    #[test]
    fn a_decision_goes_stale() {
        let mut h = DraftHold::new(Kind::Held, vec![Reason::new("a", "")], 1, "").unwrap();
        h.checked_at = "2026-11-06T22:00:00Z".into();
        let t = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .with_timezone(&chrono::Utc)
        };
        assert!(!h.stale(3600, t("2026-11-06T22:59:00Z")));
        assert!(h.stale(3600, t("2026-11-06T23:00:00Z")));
        h.checked_at = String::new();
        assert!(h.stale(3600, t("2026-11-06T23:00:00Z")));
    }
}
