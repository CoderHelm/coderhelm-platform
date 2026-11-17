//! No second change on a ticket whose work already merged.
//!
//! Most of the pull requests people closed unmerged on one team were second
//! runs on a ticket whose work had already merged: a retry, a ticket edit, or a
//! run started while the first pull request had merged into an integration
//! branch and the ticket was still open. Each one rebuilt the same change
//! from nothing.
//!
//! A merged pull request is read the way an open one is
//! (`stop_rules::existing_work`): before a run on a person's ticket (a Jira
//! key or a GitHub issue) builds anything, after its triage and the
//! ticket's own diagnosis, the ticket's newest merged pull request
//! ([`blocking`]: CoderHelm's or anyone's, into any base branch, an
//! integration branch included, never a revert) is compared with the ticket
//! by the same verdict ([`check`]):
//!
//! - it did all of what the ticket asks: the run ends covered by it
//!   (`covered_by_pr`), linked to it, with [`covered_note`]; nothing is
//!   built;
//! - it did part of it: the run goes on and builds only what is missing;
//!   the planner and the pull request description are told what merged
//!   ([`partial_note`]), and the run's pull request adds to it;
//! - it did something else (prep work, another change on the same files):
//!   the run takes its normal path;
//! - the verdict couldn't be read: the only case that waits on a person
//!   ([`unsure_note`], [`STOP_KIND`] on the record).
//!
//! The ticket asking for a separate change skips the check, as it does for
//! open pull requests. A ticket edited after its pull request merged reaches
//! the check like any other run, so new acceptance criteria get built.
//!
//! A person asking for the run after the merge skips the check and the run
//! goes ahead:
//!
//! - Retry on a run that ended covered by a merged pull request, waited on
//!   this check, or merged (the dashboard, or `POST /api/runs/{run_id}/retry`);
//! - on Jira, a reply on the ticket with what is still to do (the new run
//!   starts with that person's direction);
//! - on GitHub, `@coderhelm` (or `/coderhelm`) on the issue with what is
//!   still to do, or starting the issue from the dashboard, when the issue's
//!   latest run ended that way.
//!
//! A run started that way records it ([`FIELD_AFTER_MERGE`]), so its own
//! retries and later runs on the ticket skip the check for the same merge;
//! a pull request that merges after it is checked again.
//!
//! CoderHelm's own work (alerts, log findings, flaky tests, security fixes,
//! plan tasks) is never checked: the same finding can come back after its
//! fix merged ([`guards`]).

use serde::{Deserialize, Serialize};

use crate::messages::TicketSource;
use crate::stop_rules::existing_work::Verdict;

/// The run record's `stop_kind` for a run this rule ended: covered by a
/// merged pull request, or waiting because the check couldn't be read.
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

/// The merged pull request a new run is checked against, or None.
///
/// `allowed_runs`: ids of the ticket's earlier runs a person asked for
/// after the merge. Only the newest merge counts: a run allowed after it
/// skips the check; one allowed before it doesn't. A merge with no known
/// time is taken to be before any allowed run. When the newest merge is a
/// revert ([`MergedPr::revert`]), the ticket's work was undone and nothing
/// is checked; a pull request that merges after the revert is checked
/// again.
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

/// What the check found about the merged pull request ([`check`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// It did all of what the ticket asks: the run ends covered by it.
    Covers { reason: String },
    /// It did part of it: the run builds only the `missing` items.
    Partly {
        reason: String,
        missing: Vec<String>,
    },
    /// It did something else: the run takes its normal path.
    Unrelated { reason: String },
    /// The verdict couldn't be read: a person decides.
    Unsure,
}

/// The check, from the coverage verdict on the merged pull request
/// (`None` when it couldn't be read).
pub fn check(verdict: Option<&Verdict>) -> Check {
    match verdict {
        None => Check::Unsure,
        Some(v) if !v.addresses => Check::Unrelated {
            reason: v.reason.clone(),
        },
        Some(v) if v.missing.is_empty() => Check::Covers {
            reason: v.reason.clone(),
        },
        Some(v) => Check::Partly {
            reason: v.reason.clone(),
            missing: v.missing.clone(),
        },
    }
}

/// " into `develop` on 2026-11-02", from what is known.
fn where_when(pr: &MergedPr) -> String {
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
    format!("{into}{on}")
}

/// How a person runs the ticket anyway.
fn how(source: &TicketSource) -> &'static str {
    match source {
        TicketSource::Jira => {
            "press Retry on this run in CoderHelm, or reply on the ticket with what is still to \
             do"
        }
        _ => {
            "press Retry on this run in CoderHelm, or mention @coderhelm on the issue with what \
             is still to do"
        }
    }
}

fn link(pr: &MergedPr) -> String {
    format!("[{}]({})", pr.subject(), pr.url())
}

/// What the run page and the ticket say when the merged pull request did
/// all of what the ticket asks.
pub fn covered_note(source: &TicketSource, ticket: &str, pr: &MergedPr, reason: &str) -> String {
    let why = reason.trim().trim_end_matches('.');
    format!(
        "{ticket}'s work already merged in {}{}{}. CoderHelm didn't build a second change.\n\n\
         To run the ticket again anyway, {}.",
        link(pr),
        where_when(pr),
        if why.is_empty() {
            String::new()
        } else {
            format!(": {why}")
        },
        how(source),
    )
}

/// What the run page and the ticket say when the verdict on the merged
/// pull request couldn't be read: the one case that waits on a person.
pub fn unsure_note(source: &TicketSource, ticket: &str, pr: &MergedPr) -> String {
    format!(
        "{} merged{} for {ticket}, and CoderHelm couldn't tell whether it already does what the \
         ticket asks, so it didn't start a second change.\n\n\
         To run the ticket anyway, {}.",
        link(pr),
        where_when(pr),
        how(source),
    )
}

/// The section the planner, the coder and the pull request description get
/// when the merged pull request did part of the ticket.
pub fn partial_note(pr: &MergedPr, reason: &str, missing: &[String]) -> String {
    let why = reason.trim().trim_end_matches('.');
    let mut out = format!(
        "## Already merged for this ticket\n\n{} merged{}{}. Build only what it didn't do; \
         don't redo or revert its work. The pull request description says this change adds to \
         {}.\n\nStill to do:\n",
        pr.subject(),
        where_when(pr),
        if why.is_empty() {
            String::new()
        } else {
            format!(" and already does part of this ticket: {why}")
        },
        pr.subject(),
    );
    for m in missing {
        out.push_str(&format!("- {}\n", m.trim()));
    }
    out
}

/// What the run's wait asks for.
pub fn wait_action(source: &TicketSource) -> &'static str {
    match source {
        TicketSource::Jira => {
            "Press Retry to run the ticket anyway, or reply on the ticket with what is still to do"
        }
        _ => {
            "Press Retry to run the issue anyway, or mention @coderhelm on the issue with what is \
             still to do"
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

    fn verdict(addresses: bool, missing: &[&str]) -> Verdict {
        Verdict {
            addresses,
            reason: "same fix".into(),
            missing: missing.iter().map(|m| m.to_string()).collect(),
        }
    }

    /// The verdict decides the run: covered, partly covered, unrelated, or
    /// a person decides when it couldn't be read.
    #[test]
    fn the_verdict_decides_what_the_run_does() {
        assert_eq!(
            check(Some(&verdict(true, &[]))),
            Check::Covers {
                reason: "same fix".into()
            }
        );
        assert_eq!(
            check(Some(&verdict(true, &["the CSV export"]))),
            Check::Partly {
                reason: "same fix".into(),
                missing: vec!["the CSV export".into()]
            }
        );
        // Prep work that names the ticket ("WEB-42: add logging for the
        // investigation") is not the fix: the run goes on.
        assert_eq!(
            check(Some(&verdict(false, &[]))),
            Check::Unrelated {
                reason: "same fix".into()
            }
        );
        assert_eq!(check(None), Check::Unsure);
    }

    #[test]
    fn the_notes_say_what_merged_where_and_how_to_run_anyway() {
        let pr = merged(12, "2026-11-02T10:00:00Z");
        let jira = covered_note(
            &TicketSource::Jira,
            "WEB-42",
            &pr,
            "It adds the same index.",
        );
        assert!(
            jira.starts_with("WEB-42's work already merged in [acme/app#12](https://github.com/acme/app/pull/12) into `develop` on 2026-11-02: It adds the same index."),
            "{jira}"
        );
        assert!(jira.contains("press Retry on this run"), "{jira}");
        assert!(jira.contains("reply on the ticket"), "{jira}");
        assert!(!jira.contains("reopen"), "{jira}");
        let bare = MergedPr {
            base: String::new(),
            merged_at: String::new(),
            ..pr.clone()
        };
        let gh = covered_note(&TicketSource::Github, "GH-7", &bare, "");
        assert!(
            gh.contains("acme/app/pull/12). CoderHelm didn't build"),
            "{gh}"
        );
        assert!(gh.contains("mention @coderhelm on the issue"), "{gh}");
        let unsure = unsure_note(&TicketSource::Jira, "WEB-42", &pr);
        assert!(
            unsure.contains("couldn't tell whether it already does"),
            "{unsure}"
        );
        assert!(
            unsure.contains("merged into `develop` on 2026-11-02"),
            "{unsure}"
        );
        let part = partial_note(&pr, "It adds the index.", &["the CSV export".into()]);
        assert!(part.contains("acme/app#12 merged into `develop` on 2026-11-02 and already does part of this ticket: It adds the index."), "{part}");
        assert!(part.contains("adds to acme/app#12"), "{part}");
        assert!(part.ends_with("- the CSV export\n"), "{part}");
        assert!(wait_action(&TicketSource::Github).contains("mention @coderhelm"));
        assert!(wait_action(&TicketSource::Jira).contains("reply on the ticket"));
    }
}
