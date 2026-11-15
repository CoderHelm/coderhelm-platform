//! No second run on a ticket whose work already merged, unless a person
//! asked for one after that merge.
//!
//! Most of the pull requests people closed unmerged on one team were second
//! runs on a ticket whose work had already merged: a retry, a ticket edit, or a
//! run started while the first pull request had merged into an integration
//! branch and the ticket was still open. Each one rebuilt the same change
//! from nothing.
//!
//! The rule, before a run on a person's ticket (a Jira key or a GitHub
//! issue) does any work: when a pull request for the ticket merged
//! (CoderHelm's or anyone's, into any base branch, an integration branch
//! included), the run stops at once, says which pull request merged where
//! and when, and says how to run it anyway ([`refusal`]). It ends waiting on
//! a person (whether more work is due after a merge is theirs to
//! decide), with [`STOP_KIND`] on its record.
//!
//! A person asks for a run after the merge, explicitly, by:
//!
//! - pressing Retry on the run that stopped (the dashboard, or
//!   `POST /api/runs/{run_id}/retry`);
//! - on Jira, replying on the ticket with what is still to do (the new run
//!   starts with that person's direction);
//! - on GitHub, mentioning `@coderhelm` (or `/coderhelm`) on the issue with
//!   what is still to do, or starting the issue from the dashboard.
//!
//! A Jira status change is never one: a ticket moving In Review -> QA
//! after its pull request merged into an integration branch is the second
//! run this rule exists to stop.
//!
//! A run started that way records it ([`FIELD_AFTER_MERGE`]), so its own
//! retries and later runs on the ticket aren't stopped again by the same
//! merge ([`blocking`]). A pull request that merges after it stops them
//! again.
//!
//! CoderHelm's own work (alerts, log findings, flaky tests, security fixes,
//! plan tasks) is never stopped by this: the same finding can come back
//! after its fix merged ([`guards`]).

use serde::{Deserialize, Serialize};

use crate::messages::TicketSource;

/// The run record's `stop_kind` for a run this rule stopped.
pub const STOP_KIND: &str = "already_merged";
/// The run record's field set when a person asked for the run after the
/// ticket's pull request merged.
pub const FIELD_AFTER_MERGE: &str = "after_merge";

/// A merged pull request for the ticket.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergedPr {
    /// `owner/name`.
    pub repo: String,
    pub number: u64,
    /// The branch it merged into, when known.
    #[serde(default)]
    pub base: String,
    /// When it merged (RFC 3339), when known.
    #[serde(default)]
    pub merged_at: String,
    /// It reverted earlier work ([`is_revert`]).
    #[serde(default)]
    pub revert: bool,
}

/// A revert pull request: GitHub's Revert button titles it `Revert "…"`
/// and names its branch `revert-…`.
pub fn is_revert(title: &str, branch: &str) -> bool {
    title.trim_start().starts_with("Revert \"") || branch.starts_with("revert-")
}

impl MergedPr {
    /// `owner/name#number`.
    pub fn subject(&self) -> String {
        format!("{}#{}", self.repo, self.number)
    }

    fn url(&self) -> String {
        format!("https://github.com/{}/pull/{}", self.repo, self.number)
    }

    fn merged_ms(&self) -> Option<i64> {
        chrono::DateTime::parse_from_rfc3339(self.merged_at.trim())
            .ok()
            .map(|t| t.timestamp_millis())
    }
}

/// Whether the rule applies to the ticket: a person's Jira key or GitHub
/// issue (`GH-<number>`), never CoderHelm's own work.
pub fn guards(source: &TicketSource, ticket_id: &str) -> bool {
    match source {
        TicketSource::Jira => !ticket_id.trim().is_empty(),
        TicketSource::Github => ticket_id
            .strip_prefix("GH-")
            .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit())),
        TicketSource::Alert => false,
    }
}

/// When a run started, from its id (a ULID).
fn started_ms(run_id: &str) -> Option<i64> {
    ulid::Ulid::from_string(run_id.trim())
        .ok()
        .and_then(|u| i64::try_from(u.timestamp_ms()).ok())
}

/// The merged pull request that stops a new run, or None.
///
/// `allowed_runs`: ids of the ticket's earlier runs a person asked for
/// after the merge. Only the newest merge counts: a run allowed after it
/// lets the ticket go on; one allowed before it doesn't. A merge with no
/// known time is taken to be before any allowed run. When the newest merge
/// is a revert ([`MergedPr::revert`]), the ticket's work was undone and
/// nothing stops the run; a pull request that merges after the revert
/// stops it again.
pub fn blocking<'a>(merged: &'a [MergedPr], allowed_runs: &[&str]) -> Option<&'a MergedPr> {
    let newest = merged.iter().max_by(|a, b| {
        a.merged_ms()
            .unwrap_or(i64::MIN)
            .cmp(&b.merged_ms().unwrap_or(i64::MIN))
            .then(a.number.cmp(&b.number))
    })?;
    if newest.revert {
        return None;
    }
    let at = newest.merged_ms().unwrap_or(i64::MIN);
    let allowed_since = allowed_runs
        .iter()
        .filter_map(|id| started_ms(id))
        .any(|started| started >= at);
    (!allowed_since).then_some(newest)
}

/// What the run page and the ticket say when the rule stops a run: what
/// merged where, why no new work starts, and how to run it anyway.
pub fn refusal(source: &TicketSource, ticket: &str, pr: &MergedPr) -> String {
    let into = if pr.base.trim().is_empty() {
        String::new()
    } else {
        format!(" into `{}`", pr.base.trim())
    };
    let on = pr
        .merged_at
        .get(..10)
        .filter(|d| d.len() == 10)
        .map(|d| format!(" on {d}"))
        .unwrap_or_default();
    let how = match source {
        TicketSource::Jira => {
            "press Retry on the stopped run in CoderHelm, or reply on the ticket with what is \
             still to do"
        }
        _ => {
            "press Retry on the stopped run in CoderHelm, or mention @coderhelm on the issue \
             with what is still to do"
        }
    };
    format!(
        "CoderHelm didn't start new work on {ticket}: its pull request [{}]({}) merged{into}{on}. \
         Another run would build the same change again as a second pull request.\n\n\
         To run the ticket again anyway, {how}.",
        pr.subject(),
        pr.url(),
    )
}

/// What the run's wait asks for.
pub fn wait_action(source: &TicketSource) -> &'static str {
    match source {
        TicketSource::Jira => {
            "Press Retry to run the ticket again anyway, or reply on the ticket with what is \
             still to do"
        }
        _ => {
            "Press Retry to run the issue again anyway, or mention @coderhelm on the issue with \
             what is still to do"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merged(n: u64, at: &str) -> MergedPr {
        MergedPr {
            repo: "acme/app".into(),
            number: n,
            base: "develop".into(),
            merged_at: at.into(),
            revert: false,
        }
    }

    /// A run id (ULID) started at `rfc3339`.
    fn run_at(rfc3339: &str) -> String {
        let t = chrono::DateTime::parse_from_rfc3339(rfc3339).unwrap();
        ulid::Ulid::from_parts(t.timestamp_millis() as u64, 7).to_string()
    }

    #[test]
    fn only_peoples_tickets_are_guarded() {
        assert!(guards(&TicketSource::Jira, "WEB-42"));
        assert!(guards(&TicketSource::Github, "GH-42"));
        assert!(!guards(&TicketSource::Jira, ""));
        assert!(!guards(&TicketSource::Github, "GH-"));
        assert!(!guards(&TicketSource::Github, "E2E-FIX-abc"));
        assert!(!guards(&TicketSource::Github, "LOG-12"));
        assert!(!guards(&TicketSource::Github, "PLAN-3"));
        assert!(!guards(&TicketSource::Alert, "ALERT-9f"));
        assert!(!guards(&TicketSource::Alert, "SEC-1a"));
    }

    #[test]
    fn a_merged_pull_request_stops_the_run() {
        let prs = [merged(12, "2026-11-02T10:00:00Z")];
        assert_eq!(blocking(&prs, &[]), Some(&prs[0]));
        // Nothing merged: nothing stops it.
        assert_eq!(blocking(&[], &[]), None);
    }

    #[test]
    fn a_person_asking_after_the_merge_lets_it_run() {
        let prs = [merged(12, "2026-11-02T10:00:00Z")];
        // An earlier run a person asked for after the merge: its retries
        // and later runs go on.
        let after = run_at("2026-11-03T09:00:00Z");
        assert_eq!(blocking(&prs, &[after.as_str()]), None);
        // One asked for before the merge doesn't count.
        let before = run_at("2026-11-01T09:00:00Z");
        assert_eq!(blocking(&prs, &[before.as_str()]), Some(&prs[0]));
        // Not a run id: ignored.
        assert_eq!(blocking(&prs, &["nope"]), Some(&prs[0]));
    }

    #[test]
    fn a_newer_merge_stops_it_again() {
        let prs = [
            merged(12, "2026-11-02T10:00:00Z"),
            merged(30, "2026-11-10T10:00:00Z"),
        ];
        let between = run_at("2026-11-05T09:00:00Z");
        assert_eq!(
            blocking(&prs, &[between.as_str()]).map(|p| p.number),
            Some(30)
        );
        let later = run_at("2026-11-11T09:00:00Z");
        assert_eq!(blocking(&prs, &[later.as_str()]), None);
    }

    #[test]
    fn a_merge_with_no_known_time_is_before_any_allowed_run() {
        let prs = [merged(12, "")];
        assert_eq!(blocking(&prs, &[]), Some(&prs[0]));
        let any = run_at("2020-01-01T00:00:00Z");
        assert_eq!(blocking(&prs, &[any.as_str()]), None);
    }

    #[test]
    fn the_refusal_says_what_merged_where_and_how_to_run_anyway() {
        let pr = merged(12, "2026-11-02T10:00:00Z");
        let jira = refusal(&TicketSource::Jira, "WEB-42", &pr);
        assert!(jira.contains("WEB-42"), "{jira}");
        assert!(
            jira.contains("[acme/app#12](https://github.com/acme/app/pull/12)"),
            "{jira}"
        );
        assert!(
            jira.contains("merged into `develop` on 2026-11-02"),
            "{jira}"
        );
        assert!(jira.contains("press Retry on the stopped run"), "{jira}");
        assert!(jira.contains("reply on the ticket"), "{jira}");
        assert!(!jira.contains("reopen"), "{jira}");
        let gh = refusal(
            &TicketSource::Github,
            "GH-7",
            &MergedPr {
                base: String::new(),
                merged_at: String::new(),
                ..pr
            },
        );
        assert!(gh.contains("merged. Another run"), "{gh}");
        assert!(!gh.contains("shipped"), "{gh}");
        assert!(!gh.contains("reopen"), "{gh}");
        assert!(gh.contains("mention @coderhelm on the issue"), "{gh}");
        assert!(wait_action(&TicketSource::Github).contains("mention @coderhelm"));
        assert!(wait_action(&TicketSource::Jira).contains("reply on the ticket"));
    }
}
