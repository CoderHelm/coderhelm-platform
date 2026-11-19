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
//! ticket's own diagnosis, the ticket's merged pull requests since its
//! last revert ([`blocking`]: CoderHelm's or anyone's, into any base
//! branch, an integration branch included, never a revert; the newest
//! [`MAX_JUDGED`]) are compared with the ticket together, by the same
//! verdict ([`check`]):
//!
//! - together they did all of what the ticket asks: the run ends covered
//!   by the newest (`covered_by_pr`), linked to it, with [`covered_note`];
//!   nothing is built;
//! - they did part of it: the run goes on and builds only what is missing;
//!   the planner, the coder and the pull request description are told what
//!   merged ([`partial_note`], kept on the run's message as
//!   `merged_note` so every step has it, [`with_note`]), and the run's pull
//!   request adds to it;
//! - they did something else (prep work, another change on the same files):
//!   the run takes its normal path;
//! - the verdict couldn't be read, after one more try ([`ask_again`]): the
//!   only case that waits on a person ([`unsure_note`], [`STOP_KIND`] on
//!   the record).
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
//! - on Jira, when the ticket's latest run ended on the merge, a reply on the
//!   ticket with what is still to do (the new run starts with that person's
//!   direction); an answer to any other run gets the check like a normal run
//!   and keeps that run's "build only the rest" note ([`Carried`]);
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
/// The run record's field holding the "Already merged for this ticket"
/// section ([`partial_note`]) when the check found the merged pull requests
/// did part of the ticket: written once, so Retry and the pull request's fix
/// rounds re-apply it ([`with_note`]).
pub const FIELD_MERGED_NOTE: &str = "merged_note";

/// A run ended on a merge: the merged-ticket check ended it ([`STOP_KIND`]:
/// covered by a merged pull request, or waiting because it couldn't tell),
/// or its pull request merged.
pub fn ended_on_merge(stop_kind: &str, status: &str, pr_outcome: &str) -> bool {
    stop_kind == STOP_KIND || status == crate::run_status::MERGED || pr_outcome == "merged"
}

/// [`ended_on_merge`], from a run record.
pub fn item_ended_on_merge(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> bool {
    let f = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .map(String::as_str)
            .unwrap_or("")
    };
    ended_on_merge(f("stop_kind"), f("status"), f("pr_outcome"))
}

/// What a run started again from a person's answer on the ticket takes from
/// the run that ended: the after-merge override only when that run ended on
/// the merge (the answer is then the person asking for the work anyway),
/// and its saved "build only the rest" note, so the new run keeps it before
/// a fresh verdict. Any other answer gets the check like a normal run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Carried {
    pub after_merge: bool,
    pub note: Option<String>,
}

/// [`Carried`], from the ended run's record.
pub fn carried_from(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Carried {
    Carried {
        after_merge: item_ended_on_merge(item),
        note: carried_note(item),
    }
}

/// What already merged for the ticket, carried from a run into the next one
/// (Retry, or a person's answer on the ticket): the run's saved note, only
/// when the run didn't end on a merge, since that merge did the rest and the
/// note is out of date. One rule for every path that starts a run again.
pub fn carried_note(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Option<String> {
    if item_ended_on_merge(item) {
        return None;
    }
    note_of(item)
}

/// The run's saved [`FIELD_MERGED_NOTE`], from its record. `None` when the
/// run has none.
pub fn note_of(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Option<String> {
    item.get(FIELD_MERGED_NOTE)
        .and_then(|v| v.as_s().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

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

/// The most merged pull requests one verdict reads.
pub const MAX_JUDGED: usize = 3;

/// The merged pull requests a new run is checked against, newest first
/// (empty when nothing is checked): every one since the ticket's newest
/// revert, at most [`MAX_JUDGED`].
///
/// `allowed_runs`: ids of the ticket's earlier runs a person asked for
/// after the merge. The newest merge decides: a run allowed after it skips
/// the check; one allowed before it doesn't. A merge with no known time is
/// taken to be before any allowed run. When the newest merge is a revert
/// ([`MergedPr::revert`]), the ticket's work was undone and nothing is
/// checked; merges at or before a revert are never checked, and a pull
/// request that merges after it is.
pub fn blocking<'a>(merged: &'a [MergedPr], allowed_runs: &[&str]) -> Vec<&'a MergedPr> {
    let mut newest_first: Vec<&MergedPr> = merged.iter().collect();
    newest_first.sort_by(|a, b| {
        b.merged_ms()
            .unwrap_or(i64::MIN)
            .cmp(&a.merged_ms().unwrap_or(i64::MIN))
            .then(b.number.cmp(&a.number))
    });
    let Some(newest) = newest_first.first() else {
        return Vec::new();
    };
    let at = newest.merged_ms().unwrap_or(i64::MIN);
    let allowed_since = allowed_runs
        .iter()
        .filter_map(|id| started_ms(id))
        .any(|started| started >= at);
    if newest.revert || allowed_since {
        return Vec::new();
    }
    newest_first
        .into_iter()
        .take_while(|p| !p.revert)
        .take(MAX_JUDGED)
        .collect()
}

/// How many times the verdict is asked before a person is: a model or
/// GitHub error is tried once more.
const VERDICT_TRIES: usize = 2;

/// Whether the verdict is asked again after `tries` attempts that couldn't
/// be read.
pub fn ask_again(tries: usize) -> bool {
    tries < VERDICT_TRIES
}

/// What the check found about the merged pull requests ([`check`]).
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

/// The check, from the coverage verdict on the merged pull requests
/// together (`None` when it couldn't be read).
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

/// ", after acme/app#12 and acme/app#9" for the earlier merges judged with
/// the newest, linked or by name.
fn after(earlier: &[MergedPr], linked: bool) -> String {
    let names: Vec<String> = earlier
        .iter()
        .map(|p| if linked { link(p) } else { p.subject() })
        .collect();
    match names.as_slice() {
        [] => String::new(),
        [one] => format!(", after {one}"),
        [rest @ .., last] => format!(", after {} and {last}", rest.join(", ")),
    }
}

/// What the run page and the ticket say when the merged pull requests
/// (`prs`, newest first, never empty) did all of what the ticket asks.
pub fn covered_note(source: &TicketSource, ticket: &str, prs: &[MergedPr], reason: &str) -> String {
    let (pr, earlier) = prs.split_first().expect("a merged pull request");
    let why = reason.trim().trim_end_matches('.');
    format!(
        "{ticket}'s work already merged in {}{}{}{}. CoderHelm didn't build a second change.\n\n\
         To run the ticket again anyway, {}.",
        link(pr),
        where_when(pr),
        after(earlier, true),
        if why.is_empty() {
            String::new()
        } else {
            format!(": {why}")
        },
        how(source),
    )
}

/// What the run page and the ticket say when the verdict on the merged
/// pull requests (`prs`, newest first, never empty) couldn't be read: the
/// one case that waits on a person.
pub fn unsure_note(source: &TicketSource, ticket: &str, prs: &[MergedPr]) -> String {
    let (pr, earlier) = prs.split_first().expect("a merged pull request");
    format!(
        "{} merged{}{} for {ticket}, and CoderHelm couldn't tell whether {} already does what \
         the ticket asks, so it didn't start a second change.\n\n\
         To run the ticket anyway, {}.",
        link(pr),
        where_when(pr),
        after(earlier, true),
        if earlier.is_empty() {
            "it"
        } else {
            "that work"
        },
        how(source),
    )
}

/// The section the planner, the coder and the pull request description get
/// when the merged pull requests (`prs`, newest first, never empty) did
/// part of the ticket.
pub fn partial_note(prs: &[MergedPr], reason: &str, missing: &[String]) -> String {
    let (pr, earlier) = prs.split_first().expect("a merged pull request");
    let why = reason.trim().trim_end_matches('.');
    let mut out = format!(
        "## Already merged for this ticket\n\n{} merged{}{}{}. Build only what {} didn't do; \
         don't redo or revert {} work. The pull request description says this change adds to \
         {}.\n\nStill to do:\n",
        pr.subject(),
        where_when(pr),
        after(earlier, false),
        match (why.is_empty(), earlier.is_empty()) {
            (true, _) => String::new(),
            (false, true) => format!(" and already does part of this ticket: {why}"),
            (false, false) => format!(", and together they already do part of this ticket: {why}"),
        },
        if earlier.is_empty() { "it" } else { "they" },
        if earlier.is_empty() { "its" } else { "their" },
        pr.subject(),
    );
    for m in missing {
        out.push_str(&format!("- {}\n", m.trim()));
    }
    out
}

/// The team's `instructions` with the run's section on what already merged
/// (`TicketMessage::merged_note`, from [`partial_note`]) after them, when it
/// has one: applied each time the instructions are loaded, so every step,
/// repository and agent of the run has it.
pub fn with_note(instructions: &str, note: Option<&str>) -> String {
    match note.map(str::trim).filter(|n| !n.is_empty()) {
        None => instructions.to_string(),
        Some(n) if instructions.trim().is_empty() => n.to_string(),
        Some(n) => format!("{}\n\n{n}", instructions.trim_end()),
    }
}

/// The team's `instructions` with the run's carried section on what
/// already merged (`carried`) replaced by the fresh verdict's (`fresh`, from
/// [`partial_note`]; `None` when the verdict found the merges unrelated):
/// the instructions hold one such section at most, the verdict's.
pub fn replace_note(instructions: &str, carried: Option<&str>, fresh: Option<&str>) -> String {
    let base = match carried
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .and_then(|n| instructions.find(n).map(|i| (i, n.len())))
    {
        Some((i, len)) => {
            let head = instructions[..i].trim_end();
            let tail = instructions[i + len..].trim_start();
            match (head.is_empty(), tail.is_empty()) {
                (true, _) => tail.to_string(),
                (false, true) => head.to_string(),
                (false, false) => format!("{head}\n\n{tail}"),
            }
        }
        None => instructions.to_string(),
    };
    with_note(&base, fresh)
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
        assert_eq!(blocking(&prs, &[]), vec![&prs[0]]);
        // Nothing merged: nothing stops it.
        assert!(blocking(&[], &[]).is_empty());
    }

    #[test]
    fn a_person_asking_after_the_merge_lets_it_run() {
        let prs = [merged(12, "2026-11-02T10:00:00Z")];
        // An earlier run a person asked for after the merge: its retries
        // and later runs go on.
        let after = run_at("2026-11-03T09:00:00Z");
        assert!(blocking(&prs, &[after.as_str()]).is_empty());
        // One asked for before the merge doesn't count.
        let before = run_at("2026-11-01T09:00:00Z");
        assert_eq!(blocking(&prs, &[before.as_str()]), vec![&prs[0]]);
        // Not a run id: ignored.
        assert_eq!(blocking(&prs, &["nope"]), vec![&prs[0]]);
    }

    #[test]
    fn a_newer_merge_stops_it_again() {
        let prs = [
            merged(12, "2026-11-02T10:00:00Z"),
            merged(30, "2026-11-10T10:00:00Z"),
        ];
        let between = run_at("2026-11-05T09:00:00Z");
        let numbers = |b: Vec<&MergedPr>| b.iter().map(|p| p.number).collect::<Vec<_>>();
        // The newer merge stops it, judged with the one before it.
        assert_eq!(numbers(blocking(&prs, &[between.as_str()])), vec![30, 12]);
        let later = run_at("2026-11-11T09:00:00Z");
        assert!(blocking(&prs, &[later.as_str()]).is_empty());
    }

    /// The fix (#12) merged, then a prep or follow-up pull request naming
    /// the ticket (#30): both are judged, newest first, so the fix isn't
    /// built again because the newest one alone doesn't do the ticket.
    #[test]
    fn every_merge_since_the_last_revert_is_judged() {
        let prs = [
            merged(30, "2026-11-10T10:00:00Z"),
            merged(12, "2026-11-02T10:00:00Z"),
        ];
        let numbers = |b: Vec<&MergedPr>| b.iter().map(|p| p.number).collect::<Vec<_>>();
        assert_eq!(numbers(blocking(&prs, &[])), vec![30, 12]);
        // A merge at or before a revert isn't judged.
        let revert = MergedPr {
            revert: true,
            ..merged(20, "2026-11-05T10:00:00Z")
        };
        let with_revert = [prs[0].clone(), revert.clone(), prs[1].clone()];
        assert_eq!(numbers(blocking(&with_revert, &[])), vec![30]);
        // A revert newest of all: nothing is checked.
        let reverted = [
            prs[1].clone(),
            MergedPr {
                revert: true,
                ..merged(40, "2026-11-12T10:00:00Z")
            },
        ];
        assert!(blocking(&reverted, &[]).is_empty());
        // More than three merged: the newest three.
        let many: Vec<MergedPr> = (1..=5)
            .map(|d| merged(d, &format!("2026-11-0{d}T10:00:00Z")))
            .collect();
        assert_eq!(numbers(blocking(&many, &[])), vec![5, 4, 3]);
    }

    /// An unreadable verdict is asked once more, then a person is asked.
    #[test]
    fn an_unread_verdict_is_tried_once_more() {
        assert!(ask_again(1));
        assert!(!ask_again(2));
    }

    #[test]
    fn a_merge_with_no_known_time_is_before_any_allowed_run() {
        let prs = [merged(12, "")];
        assert_eq!(blocking(&prs, &[]), vec![&prs[0]]);
        let any = run_at("2020-01-01T00:00:00Z");
        assert!(blocking(&prs, &[any.as_str()]).is_empty());
    }

    fn verdict(addresses: bool, missing: &[&str]) -> Verdict {
        Verdict {
            addresses,
            reason: "same fix".into(),
            missing: missing.iter().map(|m| m.to_string()).collect(),
        }
    }

    /// A partial merge's section is read back from the run record, so Retry
    /// and the pull request's fix rounds keep it; a run without one has none.
    #[test]
    fn the_saved_note_is_read_back_from_the_run_record() {
        use aws_sdk_dynamodb::types::AttributeValue;
        let mut item = std::collections::HashMap::new();
        assert_eq!(note_of(&item), None);
        item.insert(
            FIELD_MERGED_NOTE.to_string(),
            AttributeValue::S("  ".into()),
        );
        assert_eq!(note_of(&item), None);
        item.insert(
            FIELD_MERGED_NOTE.to_string(),
            AttributeValue::S("## Already merged for this ticket\n".into()),
        );
        let note = note_of(&item).unwrap();
        assert_eq!(note, "## Already merged for this ticket");
        assert_eq!(
            with_note("Team rules.", Some(&note)),
            "Team rules.\n\n## Already merged for this ticket"
        );
    }

    fn record(
        pairs: &[(&str, &str)],
    ) -> std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    aws_sdk_dynamodb::types::AttributeValue::S(v.to_string()),
                )
            })
            .collect()
    }

    /// An answer on the ticket asks for the work anyway only when the run it
    /// answers ended on the merge; otherwise the new run gets the check and
    /// keeps the ended run's "build only the rest" note.
    #[test]
    fn an_answer_carries_the_override_only_after_a_merge_and_always_the_note() {
        let partial_waiting = record(&[
            ("status", "needs_input"),
            (
                FIELD_MERGED_NOTE,
                "## Already merged for this ticket\n- the CSV export",
            ),
        ]);
        let c = carried_from(&partial_waiting);
        assert!(
            !c.after_merge,
            "a run that ended waiting didn't end on the merge"
        );
        assert_eq!(
            c.note.as_deref(),
            Some("## Already merged for this ticket\n- the CSV export")
        );
        for ended in [
            record(&[("status", "covered_by_pr"), ("stop_kind", STOP_KIND)]),
            record(&[("status", "needs_input"), ("stop_kind", STOP_KIND)]),
            record(&[("status", "merged")]),
            record(&[("status", "completed"), ("pr_outcome", "merged")]),
        ] {
            assert!(carried_from(&ended).after_merge, "{ended:?}");
        }
        // A partial run whose pull request then merged: that merge did the
        // rest, so its "still to do" note is out of date and isn't carried.
        let merged_with_note = record(&[
            ("status", "merged"),
            (
                FIELD_MERGED_NOTE,
                "## Already merged for this ticket\n- the CSV export",
            ),
        ]);
        assert_eq!(
            carried_from(&merged_with_note),
            Carried {
                after_merge: true,
                note: None
            }
        );
        assert_eq!(
            carried_from(&record(&[("status", "failed")])),
            Carried::default()
        );
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

    /// The partial section follows the team's instructions each time they
    /// are loaded, while the run carries it; a run without it gets the
    /// instructions alone.
    #[test]
    fn the_partial_section_follows_every_reload() {
        let section = partial_note(
            &[merged(12, "2026-11-02T10:00:00Z")],
            "It adds the index.",
            &["the CSV export".into()],
        );
        let loaded = "Use pnpm.";
        let with = with_note(loaded, Some(&section));
        assert!(
            with.starts_with("Use pnpm.\n\n## Already merged for this ticket"),
            "{with}"
        );
        assert!(with.ends_with("- the CSV export"), "{with}");
        assert_eq!(with_note(loaded, None), loaded);
        assert_eq!(with_note(loaded, Some("  ")), loaded);
        assert_eq!(with_note("", Some(&section)), section.trim());
    }

    /// A retried run carries #12's section; its fresh verdict on #12 and
    /// #20 decides what stays: one section, the verdict's, on a partial
    /// merge; none when the merges are unrelated.
    #[test]
    fn the_fresh_verdict_replaces_the_carried_section() {
        let carried = partial_note(
            &[merged(12, "2026-11-02T10:00:00Z")],
            "It adds the index.",
            &["A".into(), "B".into()],
        );
        let fresh = partial_note(
            &[
                merged(20, "2026-11-09T10:00:00Z"),
                merged(12, "2026-11-02T10:00:00Z"),
            ],
            "They add the index and A.",
            &["B".into()],
        );
        let loaded = with_note("Use pnpm.", Some(&carried));
        let partly = replace_note(&loaded, Some(&carried), Some(&fresh));
        assert_eq!(
            partly.matches("## Already merged for this ticket").count(),
            1
        );
        assert_eq!(partly, format!("Use pnpm.\n\n{}", fresh.trim()));
        assert_eq!(replace_note(&loaded, Some(&carried), None), "Use pnpm.");
        let diagnosis = "## Ticket diagnosis\n\nRoot cause: the missing index.";
        let followed = format!("{loaded}\n\n{diagnosis}");
        let partly = replace_note(&followed, Some(&carried), Some(&fresh));
        assert_eq!(
            partly.matches("## Already merged for this ticket").count(),
            1
        );
        assert!(!partly.contains("- A\n"), "{partly}");
        assert_eq!(
            partly,
            format!("Use pnpm.\n\n{diagnosis}\n\n{}", fresh.trim())
        );
        assert_eq!(
            replace_note(&followed, Some(&carried), None),
            format!("Use pnpm.\n\n{diagnosis}")
        );
        let alone = with_note("", Some(&carried));
        assert_eq!(replace_note(&alone, Some(&carried), None), "");
        assert_eq!(
            replace_note("Use pnpm.", None, Some(&fresh)),
            with_note("Use pnpm.", Some(&fresh))
        );
    }

    #[test]
    fn the_notes_say_what_merged_where_and_how_to_run_anyway() {
        let pr = merged(12, "2026-11-02T10:00:00Z");
        let jira = covered_note(
            &TicketSource::Jira,
            "WEB-42",
            std::slice::from_ref(&pr),
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
        let gh = covered_note(&TicketSource::Github, "GH-7", &[bare], "");
        assert!(
            gh.contains("acme/app/pull/12). CoderHelm didn't build"),
            "{gh}"
        );
        assert!(gh.contains("mention @coderhelm on the issue"), "{gh}");
        let unsure = unsure_note(&TicketSource::Jira, "WEB-42", std::slice::from_ref(&pr));
        assert!(
            unsure.contains("couldn't tell whether it already does"),
            "{unsure}"
        );
        assert!(
            unsure.contains("merged into `develop` on 2026-11-02"),
            "{unsure}"
        );
        let part = partial_note(
            std::slice::from_ref(&pr),
            "It adds the index.",
            &["the CSV export".into()],
        );
        assert!(part.contains("acme/app#12 merged into `develop` on 2026-11-02 and already does part of this ticket: It adds the index."), "{part}");
        assert!(part.contains("adds to acme/app#12"), "{part}");
        assert!(part.ends_with("- the CSV export\n"), "{part}");
        // Two merges judged together: the newest is named first, the
        // earlier one after it, and the change adds to the newest.
        let both = [merged(30, "2026-11-10T10:00:00Z"), pr.clone()];
        let covered = covered_note(
            &TicketSource::Jira,
            "WEB-42",
            &both,
            "Together they add it.",
        );
        assert!(
            covered.starts_with("WEB-42's work already merged in [acme/app#30](https://github.com/acme/app/pull/30) into `develop` on 2026-11-10, after [acme/app#12](https://github.com/acme/app/pull/12): Together they add it."),
            "{covered}"
        );
        let part = partial_note(&both, "They add the index.", &["the CSV export".into()]);
        assert!(part.contains("acme/app#30 merged into `develop` on 2026-11-10, after acme/app#12, and together they already do part of this ticket: They add the index."), "{part}");
        assert!(part.contains("adds to acme/app#30."), "{part}");
        assert!(wait_action(&TicketSource::Github).contains("mention @coderhelm"));
        assert!(wait_action(&TicketSource::Jira).contains("reply on the ticket"));
    }
}
