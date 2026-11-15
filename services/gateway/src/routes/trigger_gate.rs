//! Central gate for "should this ticket event start a new run?".
//!
//! Both webhook sources route through here so the dedup semantics are
//! uniform (they used to differ per path, and the underlying queries were
//! broken — DynamoDB applies Limit before FilterExpression, so the old
//! team-partition scans only ever inspected the 5 oldest runs):
//!
//! - a run in flight (running / queued / awaiting_ci) always blocks — a new
//!   run would force-reset the branch out from under the active one
//! - a completed latest run whose PR is still open (auto-merge armed
//!   included) has handled the ticket: no new run starts. An edit that
//!   changes the ticket's CONTENT goes to that PR's feedback path
//!   ([`TicketGate::OpenPr`]); any other edit is ignored. A Jira edit one
//!   second before the merge used to start a whole duplicate run
//! - a merged latest run blocks on any ticket edit: the work shipped, and a
//!   fresh run would rebuild it from scratch as a second PR (people closed
//!   five of those in one afternoon). Only reopening the ticket (or a person
//!   asking explicitly) starts follow-up work
//! - after the PR closed without merging, a new run starts when the ticket is
//!   reopened or its content changed since CoderHelm last saw it (an edit
//!   already sent to the open PR doesn't count again)
//! - a ticket CoderHelm already handled (its latest run completed, asked a
//!   question, reported a change outside the code, or was cancelled — its PR
//!   closed without merging included) starts a new run only when the ticket
//!   CONTENT changed: a status or field change alone never starts one. The
//!   run on a cancelled ticket used to re-run on any edit and open the same
//!   PR again
//! - a run a person closed from the dashboard (`closed`) never restarts
//!   itself: a status or field change starts nothing and an edit never goes
//!   to its PR; reopening the ticket or changing its content starts a fresh
//!   run, and the closed run stays as history
//! - a ticket a person marked done on the ticket itself ("fixed in the
//!   booking system") stays done until its content changes
//! - a run CoderHelm stopped because the ticket's pull request had already
//!   merged (`common::merged_ticket`) is never followed by a run from a
//!   ticket event, an edit or a status change out of done included, so it
//!   never refuses again and again; only a person's explicit trigger
//!   (Retry on that run, a command) starts the work
//! - a failed / retried latest run doesn't block, until
//!   [`MAX_FAILED_RUNS_IN_A_ROW`] runs in a row have failed: then an event
//!   that leaves the ticket unchanged starts nothing (a field or status
//!   change used to start the same failing run every time). A content
//!   change, a reopen, a person's Retry or command still start one
//! - explicit human triggers (slash commands, comment retries) skip the
//!   content check but still respect in-flight blocking

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

use crate::AppState;

pub enum TicketGate {
    Enqueue,
    SkipInFlight,
    SkipUnchanged,
    /// The ticket's PR is open and its content changed: the change goes to
    /// that PR's feedback path, never a new run. The gate has already
    /// recorded the new content as seen.
    OpenPr(OpenPr),
}

/// The latest run's open PR, for sending a ticket edit to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenPr {
    pub run_id: String,
    pub run_status: String,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    /// The ticket text the run worked from.
    pub previous_title: String,
    pub previous_body: String,
}

/// What the latest run shows, as the gate reads it.
#[derive(Debug, Default, Clone, Copy)]
struct LatestRun<'a> {
    status: &'a str,
    pr_number: u64,
    pr_outcome: &'a str,
    /// The ticket's hash when the run started.
    context_hash: Option<&'a str>,
    /// The hash of the newest edit already sent to the run's open PR.
    seen_hash: Option<&'a str>,
    /// Why the run stopped, when it recorded one (`stop_kind`).
    stop_kind: &'a str,
    /// Runs in a row, newest first, that failed (or failed and a person
    /// retried them), this one included.
    failed_in_a_row: usize,
}

impl LatestRun<'_> {
    /// Its PR is still open (an armed auto-merge hasn't landed yet).
    fn pr_open(&self) -> bool {
        self.pr_number > 0
            && matches!(self.status, "completed" | "success")
            && !matches!(self.pr_outcome, "closed" | "merged")
    }

    fn merged(&self) -> bool {
        self.status == "merged" || self.pr_outcome == "merged"
    }

    /// It stopped because the ticket's PR had already merged.
    fn stopped_as_merged(&self) -> bool {
        self.stop_kind == common::merged_ticket::STOP_KIND
    }

    /// Its PR closed without merging. A cancelled run's PR was closed by a
    /// person (that is how most runs get cancelled); a closed run's PR may
    /// still be open, so only its recorded outcome counts.
    fn pr_closed(&self) -> bool {
        self.pr_number > 0
            && (self.pr_outcome == "closed" || self.status == common::run_status::CANCELLED)
    }

    /// A person closed the run on purpose.
    fn closed_by_person(&self) -> bool {
        self.status == common::run_status::CLOSED
    }

    /// The content changed since CoderHelm last saw it.
    fn changed(&self, new_hash: Option<&str>) -> bool {
        match (new_hash, self.seen_hash.or(self.context_hash)) {
            (Some(new), Some(seen)) => new != seen,
            // Unknown content or a legacy record: treated as unchanged.
            _ => false,
        }
    }
}

/// The gate's call on a non-explicit trigger, from the latest run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    Enqueue,
    /// Handled; the edit starts nothing. Carries the reason, for the log.
    Skip(&'static str),
    /// Send the edit to the open PR.
    ToOpenPr,
}

/// Failed runs in a row on one ticket after which an event that leaves the
/// ticket unchanged starts no further run.
pub const MAX_FAILED_RUNS_IN_A_ROW: usize = 3;

fn decide(latest: &LatestRun, new_hash: Option<&str>, reopened: bool) -> Decision {
    if !handled(latest.status) {
        // failed / retried: allow a retry, up to the cap while unchanged
        if latest.failed_in_a_row >= MAX_FAILED_RUNS_IN_A_ROW
            && !reopened
            && !latest.changed(new_hash)
        {
            return Decision::Skip(
                "the ticket's last runs failed and it is unchanged; changing the ticket, \
                 reopening it, or a person's explicit Retry or command starts another",
            );
        }
        return Decision::Enqueue;
    }
    if latest.stopped_as_merged() {
        return Decision::Skip(
            "its last run stopped because the ticket's PR had already merged; only a person's \
             Retry or command starts another",
        );
    }
    if latest.pr_open() {
        return if latest.changed(new_hash) {
            Decision::ToOpenPr
        } else {
            Decision::Skip("its PR is open and the ticket is unchanged")
        };
    }
    if latest.merged() {
        return if reopened {
            Decision::Enqueue
        } else {
            Decision::Skip("its PR merged and the ticket wasn't reopened")
        };
    }
    if (latest.pr_closed() || latest.closed_by_person()) && reopened {
        return Decision::Enqueue;
    }
    if latest.changed(new_hash) {
        Decision::Enqueue
    } else {
        Decision::Skip("handled and unchanged")
    }
}

/// A Jira update that moved the ticket back to an open status: its changelog
/// changed the status, and the status is now outside the "done" category.
pub fn jira_reopened(payload: &serde_json::Value) -> bool {
    let status_changed = payload
        .pointer("/changelog/items")
        .and_then(|v| v.as_array())
        .is_some_and(|items| {
            items.iter().any(|i| {
                i.get("field")
                    .and_then(|f| f.as_str())
                    .is_some_and(|f| f.eq_ignore_ascii_case("status"))
            })
        });
    let category = payload
        .pointer("/issue/fields/status/statusCategory/key")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    status_changed && !category.is_empty() && category != "done"
}

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// The ticket's runs, any order.
async fn ticket_runs(
    state: &AppState,
    team_id: &str,
    ticket_id: &str,
) -> Result<Vec<Item>, String> {
    state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("ticket-index")
        .key_condition_expression("team_id = :tid AND ticket_id = :ticket")
        .expression_attribute_values(":tid", attr_s(team_id))
        .expression_attribute_values(":ticket", attr_s(ticket_id))
        .send()
        .await
        .map(|out| out.items.unwrap_or_default())
        .map_err(|e| e.to_string())
}

type Item = std::collections::HashMap<String, AttributeValue>;

/// Whether the newest of a GitHub issue's runs in `repo` (`owner/name`; an
/// issue number names an issue only within its repository) stopped because
/// the issue's pull request had already merged (`common::merged_ticket`).
/// A person's explicit start on that issue (a command, the dashboard) then
/// asks for the work anyway and carries `after_merge`.
pub fn latest_stopped_as_merged(items: &[Item], repo: &str) -> bool {
    fn s<'a>(i: &'a Item, k: &str) -> &'a str {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .map(|v| v.as_str())
            .unwrap_or("")
    }
    items
        .iter()
        .filter(|i| s(i, "repo").eq_ignore_ascii_case(repo))
        .max_by(|a, b| s(a, "run_id").cmp(s(b, "run_id")))
        .is_some_and(|i| s(i, "stop_kind") == common::merged_ticket::STOP_KIND)
}

/// [`latest_stopped_as_merged`] for the GitHub issue `ticket_id` in `repo`,
/// read now. False when the runs can't be read: the worker then refuses
/// the run again with the same refusal, and Retry on it still works.
pub async fn github_issue_stopped_as_merged(
    state: &AppState,
    team_id: &str,
    ticket_id: &str,
    repo: &str,
) -> bool {
    match ticket_runs(state, team_id, ticket_id).await {
        Ok(items) => latest_stopped_as_merged(&items, repo),
        Err(e) => {
            warn!(team_id, ticket_id, error = %e, "Could not read the issue's runs");
            false
        }
    }
}

/// Decide whether a ticket trigger should enqueue a new run.
///
/// `new_context_hash`: `common::ticket_context_hash` of the incoming ticket
/// content; pass `None` when the caller cannot compute it (treated as
/// unchanged for completed tickets). `explicit` marks direct human commands,
/// which bypass the content check. `reopened`: the event moved the ticket
/// back to an open status ([`jira_reopened`]).
///
/// Fails open on query errors — the worker's own active-run guard is the
/// backstop, and dropping webhooks during a DynamoDB blip is worse than a
/// rare duplicate.
pub async fn gate_ticket_trigger(
    state: &AppState,
    team_id: &str,
    ticket_id: &str,
    new_context_hash: Option<&str>,
    explicit: bool,
    reopened: bool,
) -> TicketGate {
    let items = match ticket_runs(state, team_id, ticket_id).await {
        Ok(items) => items,
        Err(e) => {
            warn!(team_id, ticket_id, error = %e, "Trigger gate query failed — allowing run");
            return TicketGate::Enqueue;
        }
    };

    let status_of = |item: &std::collections::HashMap<String, AttributeValue>| {
        item.get("status")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };

    // In-flight only counts while plausibly ALIVE: a run stuck in "running"
    // (worker crashed pre-terminal) or an abandoned awaiting_ci would
    // otherwise block the ticket forever. Bounds: running/queued can't
    // outlive a 15-min Lambda by much (2h is generous across retries);
    // awaiting_ci is finalized by the 24h wall cap (26h adds slack).
    let now = chrono::Utc::now();
    let age_hours = |i: &std::collections::HashMap<String, AttributeValue>| {
        i.get("updated_at")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|t| now.signed_duration_since(t).num_hours())
            .unwrap_or(i64::MAX)
    };
    if items.iter().any(|i| {
        let h = age_hours(i);
        match status_of(i).as_str() {
            "running" | "queued" => h < 2,
            "awaiting_ci" => h < 26,
            _ => false,
        }
    }) {
        info!(team_id, ticket_id, "Trigger gate: run in flight — skipping");
        return TicketGate::SkipInFlight;
    }

    if explicit {
        return TicketGate::Enqueue;
    }

    // Latest run by run_id (ULIDs sort lexicographically by creation time).
    let latest = items.iter().max_by(|a, b| {
        let ra = a
            .get("run_id")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let rb = b
            .get("run_id")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        ra.cmp(&rb)
    });
    let Some(latest) = latest else {
        return TicketGate::Enqueue;
    };

    // A person marked the ticket done on the ticket itself.
    if let Some(item) = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&common::ticket_state::sk(ticket_id)))
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
    {
        let field = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).map(|s| s.as_str());
        if common::ticket_state::done_blocks_run(
            field(common::ticket_state::FIELD_STATE),
            field(common::ticket_state::FIELD_DONE_HASH),
            new_context_hash,
        ) {
            info!(
                team_id,
                ticket_id, "Trigger gate: ticket marked done and unchanged — skipping"
            );
            return TicketGate::SkipUnchanged;
        }
    }

    // "success" is a legacy terminal status from an old resume path.
    let s = |k: &str| {
        latest
            .get(k)
            .and_then(|v| v.as_s().ok())
            .map(|v| v.as_str())
    };
    let latest_status = status_of(latest);
    let run = LatestRun {
        status: &latest_status,
        pr_number: latest
            .get("pr_number")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0),
        pr_outcome: s("pr_outcome").unwrap_or(""),
        context_hash: s("context_hash"),
        seen_hash: s(SEEN_HASH_FIELD),
        stop_kind: s("stop_kind").unwrap_or(""),
        failed_in_a_row: failed_in_a_row(
            items
                .iter()
                .map(|i| {
                    let id = i.get("run_id").and_then(|v| v.as_s().ok()).cloned();
                    (id.unwrap_or_default(), status_of(i))
                })
                .collect(),
        ),
    };
    match decide(&run, new_context_hash, reopened) {
        Decision::Enqueue => {
            info!(
                team_id,
                ticket_id,
                status = %latest_status,
                reopened,
                "Trigger gate: content changed or ticket reopened — allowing a run"
            );
            TicketGate::Enqueue
        }
        // Humans can still force a run via comment, slash command, or the
        // dashboard retry.
        Decision::Skip(why) => {
            info!(team_id, ticket_id, status = %latest_status, why, "Trigger gate: skipping");
            TicketGate::SkipUnchanged
        }
        Decision::ToOpenPr => {
            let run_id = s("run_id").unwrap_or("").to_string();
            let (owner, name) = s("repo")
                .and_then(|r| r.split_once('/'))
                .unwrap_or(("", ""));
            if run_id.is_empty() || owner.is_empty() || name.is_empty() {
                info!(
                    team_id,
                    ticket_id, "Trigger gate: PR open but its run lacks a repo — ignoring the edit"
                );
                return TicketGate::SkipUnchanged;
            }
            // The edit is CoderHelm's now (the PR answers it): after the PR
            // closes, only a later change starts a run.
            if let Some(new) = new_context_hash {
                if let Err(e) = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.runs_table_name)
                    .key("team_id", attr_s(team_id))
                    .key("run_id", attr_s(&run_id))
                    .update_expression("SET #seen = :h")
                    .expression_attribute_names("#seen", SEEN_HASH_FIELD)
                    .expression_attribute_values(":h", attr_s(new))
                    .send()
                    .await
                {
                    warn!(team_id, ticket_id, error = %e, "Could not record the edit as seen");
                }
            }
            info!(
                team_id,
                ticket_id,
                run_id,
                pr = run.pr_number,
                "Trigger gate: PR open and the ticket changed — sending the edit to that PR"
            );
            TicketGate::OpenPr(OpenPr {
                run_id,
                run_status: latest_status.clone(),
                repo_owner: owner.to_string(),
                repo_name: name.to_string(),
                pr_number: run.pr_number,
                previous_title: s("title").unwrap_or("").to_string(),
                previous_body: s("body").unwrap_or("").to_string(),
            })
        }
    }
}

/// How many of the ticket's runs in a row, newest first, failed (or failed
/// and a person retried them), from `(run_id, status)` in any order. Run
/// ids are ULIDs, which sort by creation time.
fn failed_in_a_row(mut runs: Vec<(String, String)>) -> usize {
    runs.sort_by(|a, b| b.0.cmp(&a.0));
    runs.iter()
        .take_while(|(_, status)| matches!(status.as_str(), "failed" | "retried"))
        .count()
}

/// The run record's field holding the hash of the newest ticket edit sent
/// to its open PR.
const SEEN_HASH_FIELD: &str = "seen_context_hash";

/// The latest run left the ticket handled: only a change to the ticket's
/// content starts another run. "covered_by_pr": an open pull request already
/// does the work; a ticket edit asking for a separate change starts one.
/// "needs_outside_change": the fix is a change in
/// another system. "needs_input": CoderHelm asked; people answer by comment
/// or by editing the ticket. "cancelled": a person stopped it, or closed its
/// PR without merging. "closed": a person closed the run on purpose; it never
/// resumes, and only a reopened ticket or new content starts a fresh run.
/// Only a failed (or retried) run re-runs on any event.
fn handled(status: &str) -> bool {
    matches!(
        status,
        "completed"
            | "success"
            | "merged"
            | "needs_outside_change"
            | "covered_by_pr"
            | "needs_input"
            | "cancelled"
            | "closed"
    )
}

/// What a ticket edit says to the run's open PR: the text the run worked
/// from and the text now, for the worker's reader to judge.
pub fn edit_note(pr: &OpenPr, title: &str, body: &str) -> String {
    let cut = |s: &str| common::truncate_str(s.trim(), 6000).to_string();
    let mut out = format!(
        "The ticket was edited while CoderHelm's pull request #{} is open.\n\nTitle now: {}\n",
        pr.pr_number,
        title.trim()
    );
    if !pr.previous_title.trim().is_empty() && pr.previous_title.trim() != title.trim() {
        out.push_str(&format!("Title before: {}\n", pr.previous_title.trim()));
    }
    out.push_str(&format!("\nDescription now:\n{}\n", cut(body)));
    if !pr.previous_body.trim().is_empty() && pr.previous_body.trim() != body.trim() {
        out.push_str(&format!(
            "\nDescription before:\n{}\n",
            cut(&pr.previous_body)
        ));
    }
    out
}

/// Compute the S3 keys the Jira attachment upload will produce, WITHOUT
/// uploading. Must mirror `upload_image_attachments` exactly — the worker
/// hashes the uploaded keys and stores that on the run record, so the gate's
/// hash has to match.
pub fn expected_attachment_keys(
    team_id: &str,
    ticket_key: &str,
    payload: &serde_json::Value,
) -> Vec<String> {
    let safe_team = team_id.replace('#', "_");
    payload
        .get("image_attachments")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter(|att| att["data_base64"].as_str().is_some())
                .map(|att| {
                    let filename = att["filename"].as_str().unwrap_or("image.png");
                    format!("attachments/{safe_team}/{ticket_key}/{filename}")
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{
        decide, edit_note, failed_in_a_row, handled, jira_reopened, latest_stopped_as_merged,
        Decision, Item, LatestRun, OpenPr, MAX_FAILED_RUNS_IN_A_ROW,
    };
    use aws_sdk_dynamodb::types::AttributeValue;

    fn item(run_id: &str, repo: &str, stop_kind: &str) -> Item {
        let mut i = Item::new();
        i.insert("run_id".into(), AttributeValue::S(run_id.into()));
        i.insert("repo".into(), AttributeValue::S(repo.into()));
        if !stop_kind.is_empty() {
            i.insert("stop_kind".into(), AttributeValue::S(stop_kind.into()));
        }
        i
    }

    /// WEB-42's PR merged into `develop` while the ticket stayed open; the
    /// team then moved it In Review -> QA. The event reaches the worker
    /// (which refuses it: the message carries no `after_merge`), and once
    /// that refusal is the latest run the next status move starts nothing.
    #[test]
    fn a_status_move_after_a_merge_is_refused_once_then_starts_nothing() {
        let to_qa = serde_json::json!({
            "changelog": {"items": [
                {"field": "status", "fromString": "In Review", "toString": "QA"}
            ]},
            "issue": {"fields": {"status": {"statusCategory": {"key": "indeterminate"}}}}
        });
        let moved = jira_reopened(&to_qa);
        let merged = run("merged", 12, "merged", "h1");
        assert_eq!(decide(&merged, Some("h1"), moved), Decision::Enqueue);
        let refused = LatestRun {
            stop_kind: common::merged_ticket::STOP_KIND,
            ..run("needs_input", 0, "", "h1")
        };
        assert!(matches!(
            decide(&refused, Some("h1"), moved),
            Decision::Skip(_)
        ));
    }

    /// A person's command or dashboard start on a GitHub issue asks for the
    /// work after a merge only when that issue's own newest run (in its own
    /// repository: acme/api#42 is also `GH-42`) stopped for it.
    #[test]
    fn only_the_issues_own_newest_run_stopped_as_merged_counts() {
        let stop = common::merged_ticket::STOP_KIND;
        let runs = [
            item("01A", "acme/web", stop),
            item("01B", "acme/web", ""),
            item("01C", "acme/api", stop),
        ];
        assert!(latest_stopped_as_merged(&runs, "acme/api"));
        assert!(latest_stopped_as_merged(&runs, "ACME/API"));
        // acme/web's newest run (01B) didn't stop for a merge.
        assert!(!latest_stopped_as_merged(&runs, "acme/web"));
        assert!(!latest_stopped_as_merged(&runs, "acme/app"));
        assert!(!latest_stopped_as_merged(&[], "acme/api"));
    }

    fn run<'a>(status: &'a str, pr: u64, outcome: &'a str, hash: &'a str) -> LatestRun<'a> {
        LatestRun {
            status,
            pr_number: pr,
            pr_outcome: outcome,
            context_hash: Some(hash),
            seen_hash: None,
            stop_kind: "",
            failed_in_a_row: 0,
        }
    }

    /// A run stopped because the ticket's PR had already merged: no ticket
    /// event starts another (no refusal after refusal), a status change out
    /// of done included; only an explicit trigger, which skips `decide`.
    #[test]
    fn a_run_stopped_as_already_merged_starts_nothing_from_ticket_events() {
        let stopped = LatestRun {
            stop_kind: common::merged_ticket::STOP_KIND,
            ..run("needs_input", 0, "", "h1")
        };
        assert!(matches!(
            decide(&stopped, Some("h2"), false),
            Decision::Skip(_)
        ));
        assert!(matches!(
            decide(&stopped, Some("h1"), false),
            Decision::Skip(_)
        ));
        assert!(matches!(
            decide(&stopped, Some("h1"), true),
            Decision::Skip(_)
        ));
        assert!(matches!(
            decide(&stopped, Some("h2"), true),
            Decision::Skip(_)
        ));
        // Any other needs_input still runs again on a content change.
        assert_eq!(
            decide(&run("needs_input", 0, "", "h1"), Some("h2"), false),
            Decision::Enqueue
        );
    }

    /// The same failing run used to start on every field or status change.
    #[test]
    fn failed_runs_in_a_row_stop_restarting_on_an_unchanged_ticket() {
        let below = LatestRun {
            failed_in_a_row: MAX_FAILED_RUNS_IN_A_ROW - 1,
            ..run("failed", 0, "", "h1")
        };
        assert_eq!(decide(&below, Some("h1"), false), Decision::Enqueue);
        let capped = LatestRun {
            failed_in_a_row: MAX_FAILED_RUNS_IN_A_ROW,
            ..run("failed", 0, "", "h1")
        };
        assert!(matches!(
            decide(&capped, Some("h1"), false),
            Decision::Skip(_)
        ));
        assert!(matches!(decide(&capped, None, false), Decision::Skip(_)));
        // New content, or a reopen, starts one.
        assert_eq!(decide(&capped, Some("h2"), false), Decision::Enqueue);
        assert_eq!(decide(&capped, Some("h1"), true), Decision::Enqueue);
    }

    #[test]
    fn failures_in_a_row_count_from_the_newest_run() {
        let r = |id: &str, s: &str| (id.to_string(), s.to_string());
        // Out of order on purpose: ids sort by creation time.
        assert_eq!(
            failed_in_a_row(vec![
                r("01C", "failed"),
                r("01A", "failed"),
                r("01D", "retried"),
                r("01B", "completed"),
            ]),
            2
        );
        assert_eq!(failed_in_a_row(vec![r("01A", "completed")]), 0);
        assert_eq!(failed_in_a_row(vec![]), 0);
    }

    /// The prod duplicate: a Jira edit one second before the merge, while
    /// auto-merge was armed, started a whole new run.
    #[test]
    fn an_edit_while_the_pr_is_open_goes_to_that_pr_never_a_new_run() {
        let open = run("completed", 12, "", "h1");
        assert_eq!(decide(&open, Some("h2"), false), Decision::ToOpenPr);
        assert!(matches!(
            decide(&open, Some("h1"), false),
            Decision::Skip(_)
        ));
        assert!(matches!(decide(&open, Some("h1"), true), Decision::Skip(_)));
        assert!(matches!(decide(&open, None, false), Decision::Skip(_)));
        // The same edit, already sent to the PR, isn't sent again.
        let seen = LatestRun {
            seen_hash: Some("h2"),
            ..open
        };
        assert!(matches!(
            decide(&seen, Some("h2"), false),
            Decision::Skip(_)
        ));
    }

    #[test]
    fn after_a_merge_only_reopening_starts_work() {
        let merged = run("merged", 12, "merged", "h1");
        assert!(matches!(
            decide(&merged, Some("h2"), false),
            Decision::Skip(_)
        ));
        assert_eq!(decide(&merged, Some("h1"), true), Decision::Enqueue);
    }

    #[test]
    fn after_a_close_a_later_change_or_reopen_starts_work() {
        let closed = run("completed", 12, "closed", "h1");
        assert_eq!(decide(&closed, Some("h2"), false), Decision::Enqueue);
        assert_eq!(decide(&closed, Some("h1"), true), Decision::Enqueue);
        assert!(matches!(
            decide(&closed, Some("h1"), false),
            Decision::Skip(_)
        ));
        // An edit sent to the PR before it closed is not a later change.
        let seen = LatestRun {
            seen_hash: Some("h2"),
            ..closed
        };
        assert!(matches!(
            decide(&seen, Some("h2"), false),
            Decision::Skip(_)
        ));
        assert_eq!(decide(&seen, Some("h3"), false), Decision::Enqueue);
    }

    #[test]
    fn a_closed_run_never_restarts_itself() {
        // A person closed the run; its PR is still open (they kept it).
        let closed = run("closed", 12, "", "h1");
        assert!(handled("closed"));
        // A status or field change alone starts nothing, and an edit never
        // goes to the closed run's PR.
        assert!(matches!(
            decide(&closed, Some("h1"), false),
            Decision::Skip(_)
        ));
        assert!(matches!(decide(&closed, None, false), Decision::Skip(_)));
        // A reopened ticket, or new content, starts a fresh run.
        assert_eq!(decide(&closed, Some("h1"), true), Decision::Enqueue);
        assert_eq!(decide(&closed, Some("h2"), false), Decision::Enqueue);
        // Without a PR too.
        let no_pr = run("closed", 0, "", "h1");
        assert!(matches!(
            decide(&no_pr, Some("h1"), false),
            Decision::Skip(_)
        ));
        assert_eq!(decide(&no_pr, Some("h1"), true), Decision::Enqueue);
    }

    #[test]
    fn handled_without_a_pr_needs_a_content_change_and_failures_retry() {
        let asked = run("needs_input", 0, "", "h1");
        assert_eq!(decide(&asked, Some("h2"), false), Decision::Enqueue);
        assert!(matches!(
            decide(&asked, Some("h1"), false),
            Decision::Skip(_)
        ));
        assert_eq!(
            decide(&run("failed", 0, "", "h1"), Some("h1"), false),
            Decision::Enqueue
        );
    }

    #[test]
    fn a_reopen_is_a_status_change_out_of_done() {
        let ev = |cat: &str, field: &str| {
            serde_json::json!({
                "changelog": {"items": [{"field": field}]},
                "issue": {"fields": {"status": {"statusCategory": {"key": cat}}}}
            })
        };
        assert!(jira_reopened(&ev("indeterminate", "status")));
        assert!(jira_reopened(&ev("new", "Status")));
        assert!(!jira_reopened(&ev("done", "status")));
        assert!(!jira_reopened(&ev("new", "description")));
        assert!(!jira_reopened(&serde_json::json!({})));
    }

    #[test]
    fn the_edit_note_shows_before_and_after() {
        let pr = OpenPr {
            run_id: "01A".into(),
            run_status: "completed".into(),
            repo_owner: "acme".into(),
            repo_name: "api".into(),
            pr_number: 12,
            previous_title: "Fix totals".into(),
            previous_body: "Totals are wrong.".into(),
        };
        let n = edit_note(&pr, "Fix totals", "Totals are wrong on the invoice page.");
        assert!(n.contains("#12"), "{n}");
        assert!(n.contains("Description now:\nTotals are wrong on the invoice page."));
        assert!(n.contains("Description before:\nTotals are wrong."));
        assert!(!n.contains("Title before"), "{n}");
    }

    #[test]
    fn a_handled_ticket_needs_a_content_change_to_run_again() {
        // The cancelled run whose PR a person closed: a status change on the
        // ticket started the same PR again.
        assert!(handled("cancelled"));
        assert!(handled("needs_outside_change"));
        assert!(handled("covered_by_pr"));
        assert!(handled("needs_input"));
        assert!(handled("completed"));
        assert!(!handled("failed"));
        assert!(!handled("retried"));
    }
}
