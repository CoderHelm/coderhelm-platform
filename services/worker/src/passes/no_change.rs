//! A run whose plan finds that no code change is needed.
//!
//! An alarm started a run on a ticket that already had CoderHelm's open pull
//! request; the run said it would continue that pull request. Its plan found,
//! with evidence, that the code already did what the alarm asked, and the run
//! ended `completed` with no pull request, no error and no note saying why:
//! the reason lived only in the worker's log. Nobody heard back on the alarm,
//! and nothing was said about the open pull request.
//!
//! The ending is its own outcome now ([`common::run_status::NO_CHANGE`]):
//!
//! - the plan's reason, in full with what it checked, is the run's summary
//!   note and its `no_change` record, which the run page shows as "No change
//!   needed";
//! - an open pull request of CoderHelm's the run was continuing gets a
//!   decision in plain words: still needed (it stays open, and why), or no
//!   longer needed. A pull request no longer needed is closed with a note and
//!   its branch kept, the rule CoderHelm already follows for its own pull
//!   requests a newer plan supersedes (`supersede_moved_prs`). The decision
//!   goes on the run page and in the pull request's one status comment;
//! - a run an alert started answers on the alert's own channels with the
//!   reason in one paragraph, the way it posts a pull request it opened;
//! - a ticket (GitHub issue, Jira) hears the reason through the ticket's
//!   conversation layer, as every other run outcome does.
//!
//! [`decide`] holds every word and choice, and is what the incident case
//! replays; [`finish`] carries it out.

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::passes::{attr_n, attr_s, fix_location, RunUsageValues};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use common::run_ending::EndWrite;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn};

/// The sentinel the planner writes as tasks.md when the code already does it.
pub const SENTINEL: &str = "NO_CHANGES_NEEDED:";

/// The run record's field holding [`NoChange`] as JSON.
pub const FIELD: &str = "no_change";

/// What the run page calls the outcome.
pub const LABEL: &str = "No change needed";

/// What the plan decided, with the evidence, and what the run decided about
/// an open pull request it was continuing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoChange {
    /// The plan's reason, in full.
    pub reason: String,
    /// What the plan checked, one line each.
    #[serde(default)]
    pub checks: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_pr: Option<PrCall>,
}

/// Whether the open pull request is still needed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Still needed: it stays open.
    #[default]
    Keep,
    /// The code already does what it was for.
    NotNeeded,
}

/// CoderHelm's open pull request for the ticket, as the run found it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenPr {
    /// `owner/name`.
    pub repo: String,
    pub number: u64,
    pub url: String,
    pub title: String,
    pub branch: String,
}

/// The decision about the open pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrCall {
    pub repo: String,
    pub number: u64,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub branch: String,
    pub verdict: Verdict,
    #[serde(default)]
    pub why: String,
    /// CoderHelm closed it (only when not needed).
    #[serde(default)]
    pub closed: bool,
    /// The decision in one sentence, as the run page and the pull request
    /// say it.
    #[serde(default)]
    pub line: String,
}

/// The run's reason from tasks.md: everything after the sentinel.
pub fn reason_of(tasks: &str) -> String {
    tasks
        .trim_start()
        .strip_prefix(SENTINEL)
        .unwrap_or("")
        .trim()
        .to_string()
}

/// What the plan checked: each lookup, the rule the code follows and what
/// the data shows, as the planner recorded them.
pub fn checks_of(fix: Option<&fix_location::FixDecision>) -> Vec<String> {
    let Some(f) = fix else {
        return Vec::new();
    };
    let mut out: Vec<String> = f
        .evidence
        .looked_up
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if !f.evidence.code_rule.trim().is_empty() {
        out.push(format!("The code's rule: {}", f.evidence.code_rule.trim()));
    }
    if !f.evidence.data_shows.trim().is_empty() {
        out.push(format!(
            "What the data shows: {}",
            f.evidence.data_shows.trim()
        ));
    }
    out.dedup();
    out
}

/// The reason's first paragraph: what an alert's channel gets.
pub fn first_paragraph(reason: &str) -> String {
    reason
        .trim()
        .split("\n\n")
        .map(str::trim)
        .find(|p| !p.is_empty())
        .unwrap_or("")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// The decision about the open pull request, in one sentence, from what
/// happened to it.
pub fn pr_line(call: &PrCall) -> String {
    let why = call.why.trim().trim_end_matches('.');
    let why = if why.is_empty() {
        String::new()
    } else {
        format!(": {why}")
    };
    match (call.verdict, call.closed) {
        (Verdict::Keep, _) => format!("Pull request #{} stays open{why}.", call.number),
        (Verdict::NotNeeded, true) => format!(
            "Pull request #{} is no longer needed, so CoderHelm closed it{why}. Its branch `{}` is kept.",
            call.number, call.branch
        ),
        (Verdict::NotNeeded, false) => format!(
            "Pull request #{} is no longer needed{why}. CoderHelm could not close it; closing it is recommended.",
            call.number
        ),
    }
}

/// What the run says on the open pull request, in its one status comment.
pub fn pr_status(call: &PrCall, reason: &str) -> String {
    let first = first_paragraph(reason);
    let found = common::truncate_str(&first, 600);
    let head = format!("A later run for this ticket found that no code change is needed: {found}");
    match call.verdict {
        Verdict::Keep => format!(
            "{head}\n\nThis pull request stays open: {}",
            call.why.trim()
        ),
        Verdict::NotNeeded => format!(
            "Superseded. {head}\n\n{} This pull request is closed. Its branch `{}` is kept.",
            call.why.trim(),
            call.branch
        ),
    }
}

/// The run's summary note: the reason in full, what was checked, and the
/// pull request decision.
pub fn summary_note(nc: &NoChange) -> String {
    let mut out = format!("{LABEL}. {}", nc.reason.trim());
    if !nc.checks.is_empty() {
        out.push_str("\n\nChecked:");
        for c in &nc.checks {
            out.push_str(&format!("\n- {c}"));
        }
    }
    if let Some(call) = &nc.open_pr {
        out.push_str(&format!("\n\n{}", call.line));
    }
    out
}

/// What the ticket hears (before the team's voice is applied).
pub fn ticket_text(nc: &NoChange) -> String {
    let mut out = format!(
        "No change needed: {}\n\nThe code already does what this ticket asks for, so CoderHelm opened no pull request.",
        nc.reason.trim()
    );
    if let Some(call) = &nc.open_pr {
        out.push_str(&format!("\n\n{}", call.line));
    }
    out
}

/// The alert's channels: one paragraph of reason, and the pull request
/// decision.
pub fn alert_reply(nc: &NoChange) -> (String, String) {
    (
        first_paragraph(&nc.reason),
        nc.open_pr
            .as_ref()
            .map(|c| c.line.clone())
            .unwrap_or_default(),
    )
}

/// Everything the ending does, decided from what the run has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ending {
    /// The run's status.
    pub status: &'static str,
    /// The run's `no_change` record.
    pub record: NoChange,
    /// The run's summary note (the reason, the checks, the PR decision).
    pub note: String,
    /// The open pull request's status line and whether to close it.
    pub on_pr: Option<(u64, String, bool)>,
    /// For a run an alert started: the reply on its channels (reason
    /// paragraph, pull request line).
    pub alert: Option<(String, String)>,
    /// For a run a ticket started: the text for the ticket.
    pub ticket: Option<String>,
}

/// Where the run came from, as the ending answers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// An alert: its channels hear back.
    Alert,
    /// A GitHub issue or a Jira ticket: it hears back on the ticket.
    Ticket,
    /// Nothing to answer (a run started from the dashboard, say).
    Neither,
}

/// Where `msg` came from.
pub fn origin_of(msg: &TicketMessage) -> Origin {
    match msg.source {
        TicketSource::Alert => Origin::Alert,
        TicketSource::Github if msg.issue_number > 0 => Origin::Ticket,
        TicketSource::Jira if !msg.ticket_id.is_empty() => Origin::Ticket,
        _ => Origin::Neither,
    }
}

/// Decides the ending. `verdict` is the reading of the open pull request
/// (`None` when there is none); `closed` whether closing it worked (only
/// read for a pull request not needed).
pub fn decide(
    origin: Origin,
    tasks: &str,
    fix: Option<&fix_location::FixDecision>,
    open_pr: Option<&OpenPr>,
    verdict: Option<(Verdict, String)>,
    closed: bool,
) -> Ending {
    let mut record = NoChange {
        reason: reason_of(tasks),
        checks: checks_of(fix),
        open_pr: None,
    };
    if record.reason.is_empty() {
        record.reason = "The plan found the code already does what was asked.".into();
    }
    if let Some(pr) = open_pr {
        let (verdict, why) = verdict.unwrap_or((
            Verdict::Keep,
            "the run could not settle whether its change is still needed, so it is left as it is"
                .into(),
        ));
        let mut call = PrCall {
            repo: pr.repo.clone(),
            number: pr.number,
            url: pr.url.clone(),
            title: pr.title.clone(),
            branch: pr.branch.clone(),
            verdict,
            why,
            closed: verdict == Verdict::NotNeeded && closed,
            line: String::new(),
        };
        call.line = pr_line(&call);
        record.open_pr = Some(call);
    }
    let on_pr = record.open_pr.as_ref().map(|c| {
        (
            c.number,
            pr_status(c, &record.reason),
            c.verdict == Verdict::NotNeeded,
        )
    });
    let alert = (origin == Origin::Alert).then(|| alert_reply(&record));
    let ticket = (origin == Origin::Ticket).then(|| ticket_text(&record));
    Ending {
        status: common::run_status::NO_CHANGE,
        note: summary_note(&record),
        record,
        on_pr,
        alert,
        ticket,
    }
}

/// The model's instructions for reading whether the open pull request is
/// still needed.
pub const JUDGE_SYSTEM: &str = "CoderHelm, an automated coding agent, has an open pull request \
     for a ticket from an earlier run. A new run on the same ticket planned again from the \
     current code and found that no code change is needed, for the reason given. You decide \
     whether the open pull request is still needed. keep: its change is not in the code yet and \
     the ticket (or the alert behind it) still needs it, or the reason does not speak to what \
     it changes. not_needed: the reason shows the code already does what the pull request was \
     for, or what it was for no longer happens. Judge from the reason and the pull request's \
     own description, never from its age. why: one or two plain sentences a person reads on \
     the run page, naming the fact the decision rests on.";

pub fn judge_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "verdict": {"type": "string", "enum": ["keep", "not_needed"]},
            "why": {"type": "string"}
        },
        "required": ["verdict", "why"],
        "additionalProperties": false
    })
}

pub fn judge_prompt(reason: &str, pr: &OpenPr, pr_body: &str) -> String {
    format!(
        "## Why the new run found no code change is needed\n{}\n\n## The open pull request #{}: {}\n{}\n",
        common::truncate_str(reason.trim(), 6000),
        pr.number,
        pr.title.trim(),
        common::truncate_str(pr_body.trim(), 4000)
    )
}

/// The model's answer, read; `None` when it can't be.
pub fn parse_verdict(v: &Value) -> Option<(Verdict, String)> {
    let why = v["why"].as_str().unwrap_or("").trim().to_string();
    if why.is_empty() {
        return None;
    }
    match v["verdict"].as_str()? {
        "keep" => Some((Verdict::Keep, why)),
        "not_needed" => Some((Verdict::NotNeeded, why)),
        _ => None,
    }
}

async fn judge(
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    reason: &str,
    pr: &OpenPr,
    body: &str,
) -> Option<(Verdict, String)> {
    match crate::agent::anthropic::converse_json(
        &provider.client(),
        provider.judgment_model_id(),
        JUDGE_SYSTEM,
        &judge_prompt(reason, pr, body),
        &judge_schema(),
        true,
        common::stop_judge::JUDGMENT_EFFORT,
        4096,
        usage,
    )
    .await
    {
        Ok(v) => parse_verdict(&v),
        Err(e) => {
            warn!(error = %e, "Could not read whether the open pull request is still needed");
            None
        }
    }
}

/// The ticket's open pull request of CoderHelm's in this repository, read
/// from GitHub (still open, opened by CoderHelm's app), with its body.
async fn open_pr_now(
    github: &GitHubClient,
    msg: &TicketMessage,
    history: &super::ticket_history::TicketHistory,
) -> Option<(OpenPr, String)> {
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let own = history.own_open_pr(&repo)?;
    let pr = github
        .get_pull_request(&msg.repo_owner, &msg.repo_name, own.number)
        .await
        .ok()?;
    if pr["state"].as_str() != Some("open")
        || !super::is_own_login(pr["user"]["login"].as_str().unwrap_or(""))
    {
        return None;
    }
    Some((
        OpenPr {
            repo: repo.clone(),
            number: own.number,
            url: pr["html_url"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| format!("https://github.com/{repo}/pull/{}", own.number)),
            title: pr["title"].as_str().unwrap_or(&own.title).to_string(),
            branch: pr["head"]["ref"]
                .as_str()
                .unwrap_or(&own.branch)
                .to_string(),
        },
        pr["body"].as_str().unwrap_or("").to_string(),
    ))
}

/// What [`finish`] needs from the run.
pub(crate) struct Run<'a> {
    pub state: &'a WorkerState,
    pub msg: &'a TicketMessage,
    pub run_id: &'a str,
    pub github: &'a GitHubClient,
    pub provider: &'a ModelProvider,
    pub voice: &'a str,
    pub history: &'a super::ticket_history::TicketHistory,
    /// The repository CoderHelm picked itself, and why.
    pub auto_pick: Option<&'a (String, String)>,
    pub start: &'a std::time::Instant,
    pub mcp_ids: &'a [String],
}

/// Ends the run as [`common::run_status::NO_CHANGE`]: the reason on the run
/// page, the open pull request decided, the alert or the ticket answered.
pub(crate) async fn finish(run: Run<'_>, plan: &super::plan::PlanResult, usage: &mut TokenUsage) {
    let Run {
        state,
        msg,
        run_id,
        github,
        provider,
        voice,
        history,
        auto_pick,
        start,
        mcp_ids,
    } = run;
    let reason = reason_of(&plan.tasks);
    info!(run_id, reason = %reason, "Plan determined no changes needed");

    // The open pull request this run was continuing: read it, decide it,
    // and close it when it is no longer needed.
    let open = open_pr_now(github, msg, history).await;
    let verdict = match &open {
        Some((pr, body)) => judge(provider, usage, &reason, pr, body).await,
        None => None,
    };
    let origin = origin_of(msg);
    let mut end = decide(
        origin,
        &plan.tasks,
        plan.fix.as_ref(),
        open.as_ref().map(|(p, _)| p),
        verdict.clone(),
        true,
    );
    if let (Some((number, text, close)), Some((pr, _))) = (end.on_pr.clone(), open.as_ref()) {
        let say = if close {
            common::conversation::Say::note("superseded", &text)
        } else {
            common::conversation::Say::status("run", &text)
        };
        let said =
            super::conversation::github(github, &msg.repo_owner, &msg.repo_name, number, say).await;
        if let Err(e) = &said {
            warn!(run_id, pr = number, error = %e, "Could not post the decision on the open pull request");
        }
        let mut closed = false;
        if close && said.is_ok() {
            match github
                .close_pull_request(&msg.repo_owner, &msg.repo_name, number)
                .await
            {
                Ok(_) => {
                    closed = true;
                    info!(
                        run_id,
                        pr = number,
                        "Closed own pull request: no longer needed"
                    );
                }
                Err(e) => {
                    warn!(run_id, pr = number, error = %e, "Could not close own pull request")
                }
            }
        }
        if close && !closed {
            end = decide(
                origin,
                &plan.tasks,
                plan.fix.as_ref(),
                Some(pr),
                verdict.clone(),
                false,
            );
        }
    }

    // The run page: the summary note, then (when CoderHelm picked the
    // repository itself) which one it read and why.
    super::add_progress_note(state, &msg.team_id, run_id, &end.note).await;
    let repo_note = auto_pick.map(|(repo, why)| super::repo_pick::no_changes_repo_note(repo, why));
    if let Some(note) = &repo_note {
        super::add_progress_note(state, &msg.team_id, run_id, note).await;
    }

    // The ticket: through its conversation layer, which reads the thread
    // first and answers people who wrote since.
    if let Some(text) = &end.ticket {
        let mut body =
            super::formatter::format_with_voice(state, voice, text, provider, usage).await;
        // After the voice: neither can be reworded away.
        if let Some(call) = &end.record.open_pr {
            if !body.contains(&call.line) {
                body = format!("{body}\n\n{}", call.line);
            }
        }
        if let Some(note) = &repo_note {
            body = format!("{body}\n\n{note}");
        }
        let judge = super::conversation::ModelJudge::new(provider, usage);
        if matches!(msg.source, TicketSource::Github) {
            let _ = super::conversation::github_judged(
                github,
                &msg.repo_owner,
                &msg.repo_name,
                msg.issue_number,
                common::conversation::Say::status("run", &body),
                Some(&judge),
            )
            .await;
        } else {
            let _ = super::conversation::jira_judged(
                state,
                &msg.team_id,
                &msg.ticket_id,
                common::conversation::Say::status("run", &body),
                "no_changes",
                LABEL,
                Some(&judge),
            )
            .await;
        }
    }

    // The alert: a reply on its own channels.
    if let Some((paragraph, pr_line)) = &end.alert {
        super::pr::notify_alert_no_change(state, msg, run_id, paragraph, pr_line).await;
    }

    // The record.
    let duration = start.elapsed().as_secs();
    let delta = usage.unrecorded();
    let now = chrono::Utc::now().to_rfc3339();
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, tokens_in = if_not_exists(tokens_in, :uzero) + :ti, tokens_out = if_not_exists(tokens_out, :uzero) + :to, \
             cache_read_tokens = if_not_exists(cache_read_tokens, :uzero) + :crt, cache_write_tokens = if_not_exists(cache_write_tokens, :uzero) + :cwt, \
             cost_usd = if_not_exists(cost_usd, :uzero) + :c, \
             duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri, mcp_servers = :mcp, #nc = :nc",
        )
        .condition_expression(common::run_status::not_ended("#status"))
        .expression_attribute_names("#status", "status")
        .expression_attribute_names("#nc", FIELD)
        // Done with the reason no change was needed (`common::run_ending`).
        .run_status(
            ":s",
            &common::run_ending::gate(
                run_id,
                &common::run_ending::RunEnd::Done(common::run_ending::Done::NoChange {
                    why: end.note.clone(),
                }),
            ),
        )
        .usage_values(&delta)
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(
            ":mcp",
            AttributeValue::L(mcp_ids.iter().map(|s| attr_s(s)).collect()),
        )
        .expression_attribute_values(
            ":nc",
            attr_s(&serde_json::to_string(&end.record).unwrap_or_default()),
        );
    let res = common::run_status::EndedValues::ended_values(res)
        .send()
        .await;
    match res {
        Ok(_) => usage.mark_recorded(&delta),
        Err(e) => warn!(run_id, error = %e, "Could not record the no-change ending"),
    }
    super::webhooks::emit_run(state, &msg.team_id, run_id, end.status).await;

    // Analytics: a finished run, counted apart from the ones that delivered
    // a pull request.
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("period", attr_s(period))
            .update_expression("ADD completed :one, no_change :one")
            .expression_attribute_values(":one", attr_n(1))
            .send()
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pr() -> OpenPr {
        OpenPr {
            repo: "acme/site".into(),
            number: 12,
            url: "https://github.com/acme/site/pull/12".into(),
            title: "Block the scraper".into(),
            branch: "coderhelm/t-1".into(),
        }
    }

    #[test]
    fn the_reason_and_checks_come_from_the_plan() {
        let mut fix = fix_location::FixDecision::default();
        fix.evidence.looked_up = vec!["logs: 40 requests".into(), " ".into()];
        fix.evidence.data_shows = "under the threshold".into();
        let end = decide(
            Origin::Ticket,
            "NO_CHANGES_NEEDED: The rule already matches.\n\nSecond paragraph.",
            Some(&fix),
            None,
            None,
            true,
        );
        assert_eq!(end.status, common::run_status::NO_CHANGE);
        assert_eq!(
            end.record.reason,
            "The rule already matches.\n\nSecond paragraph."
        );
        assert_eq!(
            end.record.checks,
            vec![
                "logs: 40 requests",
                "What the data shows: under the threshold"
            ]
        );
        assert!(end
            .note
            .starts_with("No change needed. The rule already matches."));
        assert!(end.note.contains("Second paragraph."));
        assert!(end.note.contains("- logs: 40 requests"));
        assert!(end.ticket.unwrap().contains("The rule already matches."));
        assert!(end.alert.is_none());
        assert!(end.on_pr.is_none());
    }

    #[test]
    fn the_open_pull_request_is_decided_in_plain_words() {
        let keep = decide(
            Origin::Ticket,
            "NO_CHANGES_NEEDED: done",
            None,
            Some(&pr()),
            Some((Verdict::Keep, "its change is not in the code yet".into())),
            true,
        );
        let call = keep.record.open_pr.clone().unwrap();
        assert_eq!(
            call.line,
            "Pull request #12 stays open: its change is not in the code yet."
        );
        let (n, text, close) = keep.on_pr.unwrap();
        assert_eq!((n, close), (12, false));
        assert!(text.contains("This pull request stays open: its change is not in the code yet"));
        assert!(keep.ticket.unwrap().contains(&call.line));

        let gone = decide(
            Origin::Alert,
            "NO_CHANGES_NEEDED: done",
            None,
            Some(&pr()),
            Some((Verdict::NotNeeded, "The traffic it blocked stopped.".into())),
            true,
        );
        let call = gone.record.open_pr.clone().unwrap();
        assert!(call.closed);
        assert_eq!(
            call.line,
            "Pull request #12 is no longer needed, so CoderHelm closed it: The traffic it blocked stopped. Its branch `coderhelm/t-1` is kept."
        );
        assert!(gone.on_pr.unwrap().2);
        let failed = decide(
            Origin::Alert,
            "NO_CHANGES_NEEDED: done",
            None,
            Some(&pr()),
            Some((Verdict::NotNeeded, "x".into())),
            false,
        );
        assert!(failed
            .record
            .open_pr
            .unwrap()
            .line
            .contains("closing it is recommended"));
        // No reading of it: it stays as it is, and the run says so.
        let unread = decide(
            Origin::Alert,
            "NO_CHANGES_NEEDED: done",
            None,
            Some(&pr()),
            None,
            true,
        );
        let call = unread.record.open_pr.unwrap();
        assert_eq!(call.verdict, Verdict::Keep);
        assert!(!call.closed);
        assert!(call.line.starts_with("Pull request #12 stays open: "));
    }

    #[test]
    fn verdicts_are_read_strictly() {
        assert_eq!(
            parse_verdict(&json!({"verdict": "not_needed", "why": "gone"})),
            Some((Verdict::NotNeeded, "gone".into()))
        );
        assert_eq!(parse_verdict(&json!({"verdict": "keep", "why": " "})), None);
        assert_eq!(
            parse_verdict(&json!({"verdict": "maybe", "why": "x"})),
            None
        );
    }

    #[test]
    fn the_record_round_trips_for_the_run_page() {
        let end = decide(
            Origin::Alert,
            "NO_CHANGES_NEEDED: done",
            None,
            Some(&pr()),
            Some((Verdict::Keep, "still needed".into())),
            true,
        );
        let s = serde_json::to_string(&end.record).unwrap();
        assert!(s.contains("\"verdict\":\"keep\""), "{s}");
        let back: NoChange = serde_json::from_str(&s).unwrap();
        assert_eq!(back, end.record);
    }

    #[test]
    fn the_alert_gets_one_paragraph() {
        assert_eq!(
            first_paragraph("  First line\nwraps here.\n\nSecond."),
            "First line wraps here."
        );
    }
}
