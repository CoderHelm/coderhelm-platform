//! Team lessons on where a fix belongs.
//!
//! When people close CoderHelm's pull request without merging it, or say on
//! the PR or the ticket that the fix was somewhere else (a setting in the
//! booking system, an entry in the CMS), a model reads what happened and, when
//! the fix did belong outside this code, writes one lesson for the next
//! similar ticket: "Classes missing from the site are often because the
//! semester is not available on web in the booking system; check that first."
//! It is stored in the repository's memory (the same store the planner
//! recalls from at the start of every run), tagged so the planner sees the
//! team's lessons on their own, ahead of other learnings.

use crate::agent::provider::{self, ModelProvider};
use crate::clients::github::GitHubClient;
use crate::memory::{AgentMemory, LearningOrigin};
use crate::models::{LessonMessage, TokenUsage};
use crate::passes::{attr_s, fix_location::FixDecision};
use crate::WorkerState;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{info, warn};

/// Re-tries while another run holds the repository's memory.
const MAX_ATTEMPTS: u32 = 8;
const RETRY_DELAY_SECS: i32 = 120;
/// Most text from people handed to the judge.
const MAX_COMMENT_CHARS: usize = 6000;

pub const TRIGGER_PR_CLOSED: &str = "pr_closed";
pub const TRIGGER_COMMENT: &str = "comment";
/// A person answered CoderHelm's question on a PR closed without a reason.
pub const TRIGGER_CLOSE_REASON: &str = common::run_status::CLOSE_REASON_TRIGGER;

/// The outcome signal on a run whose PR a person closed without saying why,
/// and once they answered (`close_signal` on the run record).
pub const SIGNAL_NO_REASON: &str = "closed_no_reason";
pub const SIGNAL_WITH_REASON: &str = "closed_with_reason";

/// Marks CoderHelm's one question on a PR closed without a reason.
pub const CLOSE_ASK_MARKER: &str = "<!-- coderhelm:close-reason -->";

/// The one short question CoderHelm asks on a PR closed without a reason.
pub fn close_reason_question() -> String {
    format!(
        "{CLOSE_ASK_MARKER}\nThanks for looking at this. What was wrong with it? One line here \
         (wrong approach, not needed, the fix belonged somewhere else, ...) helps me get the next \
         ticket like this right. I won't ask again."
    )
}

/// Whether people said why the PR was closed: a comment on it or on the
/// ticket's issue, or the reason picked when the run was closed in CoderHelm.
pub fn has_reason(ctx: &LessonContext, run_close_reason: &str) -> bool {
    !run_close_reason.trim().is_empty() || ctx.comments.iter().any(|(_, t)| !t.trim().is_empty())
}

/// What the judge is shown.
#[derive(Debug, Default, Clone)]
pub struct LessonContext {
    pub ticket_title: String,
    pub ticket_body: String,
    /// What the planner found (see `fix_location::reviewer_context`).
    pub plan_findings: String,
    pub pr_title: String,
    pub pr_body: String,
    /// What people wrote, as (author, text).
    pub comments: Vec<(String, String)>,
    /// The PR was closed without being merged.
    pub closed_unmerged: bool,
}

/// The judge's structured answer.
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize)]
pub struct Judgment {
    /// People indicate the fix belonged outside this code (data or settings
    /// in another system), or the plan's findings show it and the PR worked
    /// around that data.
    #[serde(default)]
    pub fix_was_elsewhere: bool,
    /// The system the fix belonged in.
    #[serde(default, rename = "where")]
    pub where_: String,
    /// One general lesson for the next similar ticket (empty when none).
    #[serde(default)]
    pub lesson: String,
    /// The person's comment also asks for a change to this PR's code.
    #[serde(default)]
    pub also_wants_code_change: bool,
}

impl Judgment {
    /// The lesson to remember, if there is one.
    pub fn lesson_to_store(&self) -> Option<&str> {
        let l = self.lesson.trim();
        (self.fix_was_elsewhere && !l.is_empty()).then_some(l)
    }
}

const SYSTEM: &str = "You read what happened to an automated coding agent's pull request and \
     decide whether the fix belonged outside the code: in data or settings held in another \
     system (a booking or scheduling system, a CMS, a ticket tool's configuration, a feature \
     flag service, a cloud console). Say yes only when people say so (\"the fix was in the \
     booking system\", \"this was a setting, not code\") or when the planner's findings show the \
     code works as designed, the data explains the symptom, and the pull request overrode that \
     data in code (for example a hardcoded list of record ids). A PR closed for any other reason \
     (duplicate, superseded, wrong approach inside the code, no longer needed) is not one. \
     When yes, write ONE lesson for the next similar ticket on this repository: start from the \
     symptom people report, name the system and the setting to check first, in plain words, as \
     a general rule. No record ids, names of people or customers, dates, or ticket numbers. \
     Example: \"Classes missing from the site are often because their semester is not available \
     on the web in the booking system; check that setting first.\" \
     Output ONLY a fenced ```json block: {\"fix_was_elsewhere\": bool, \"where\": \"the system, \
     or empty\", \"lesson\": \"the lesson, or empty\", \"also_wants_code_change\": bool \
     (a comment that also asks for a change to this PR's code)}.";

/// The judge's prompt for one PR.
pub fn prompt(ctx: &LessonContext) -> String {
    let mut out = format!(
        "## Ticket\n{}\n\n{}\n",
        ctx.ticket_title.trim(),
        common::truncate_str(ctx.ticket_body.trim(), 3000)
    );
    if !ctx.plan_findings.trim().is_empty() {
        out.push_str(&ctx.plan_findings);
        out.push('\n');
    }
    out.push_str(&format!(
        "\n## Pull request\n{}\n\n{}\n",
        ctx.pr_title.trim(),
        common::truncate_str(ctx.pr_body.trim(), 3000)
    ));
    if ctx.closed_unmerged {
        out.push_str("\nThe pull request was closed without being merged.\n");
    }
    if !ctx.comments.is_empty() {
        out.push_str("\n## What people wrote\n");
        let mut used = 0;
        for (author, text) in ctx.comments.iter().rev() {
            if used > MAX_COMMENT_CHARS {
                break;
            }
            let t = common::truncate_str(text.trim(), 1500);
            used += t.len();
            out.push_str(&format!("- {author}: {t}\n"));
        }
    }
    out
}

/// Reads the judge's reply; `None` when it didn't answer in the shape asked.
pub fn parse_judgment(text: &str) -> Option<Judgment> {
    let block = match text.rfind("```json") {
        Some(s) => {
            let after = &text[s + 7..];
            after[..after.find("```").unwrap_or(after.len())].trim()
        }
        None => text.trim(),
    };
    serde_json::from_str(block).ok()
}

/// Asks the model whether the fix belonged outside the code, and what to learn.
pub async fn judge(
    state: &WorkerState,
    provider: &ModelProvider,
    ctx: &LessonContext,
    usage: &mut TokenUsage,
) -> Option<Judgment> {
    let reply = provider::converse_simple(
        state,
        provider,
        provider.primary_model_id(),
        SYSTEM,
        &prompt(ctx),
        usage,
    )
    .await;
    match reply {
        Ok(text) => {
            let j = parse_judgment(&text);
            if j.is_none() {
                warn!("Lesson judge's reply didn't read");
            }
            j
        }
        Err(e) => {
            warn!(error = %e, "Lesson judge failed");
            None
        }
    }
}

type Item = HashMap<String, aws_sdk_dynamodb::types::AttributeValue>;

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

async fn load_run(state: &WorkerState, team_id: &str, run_id: &str) -> Option<Item> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
}

/// The ticket and planner's findings recorded on the run.
fn run_context(item: &Item) -> LessonContext {
    let plan_findings = serde_json::from_str::<FixDecision>(&s(item, "fix_decision"))
        .map(|d| super::fix_location::reviewer_context(&d))
        .unwrap_or_default();
    let mut body = s(item, "issue_body");
    if body.is_empty() {
        body = s(item, "body");
    }
    LessonContext {
        ticket_title: s(item, "title"),
        ticket_body: body,
        plan_findings,
        ..Default::default()
    }
}

/// A person's comment on CoderHelm's PR (or the ticket behind it) during
/// feedback. When it says the fix belongs outside the code, the lesson is
/// recorded, the person hears that CoderHelm understood, and — unless they
/// also asked for a code change — feedback stops there instead of changing
/// code to answer it. Returns the judgment when feedback should stop.
pub async fn check_comment(
    state: &WorkerState,
    provider: &ModelProvider,
    github: &GitHubClient,
    msg: &crate::models::FeedbackMessage,
    usage: &mut TokenUsage,
) -> Option<Judgment> {
    let mut text = msg.review_body.trim().to_string();
    for c in &msg.comments {
        if !c.body.trim().is_empty() {
            text.push_str("\n\n");
            text.push_str(c.body.trim());
        }
    }
    if text.trim().is_empty() {
        return None;
    }
    let item = load_run(state, &msg.team_id, &msg.run_id).await?;
    let mut ctx = run_context(&item);
    if let Ok(pr) = github
        .get_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        ctx.pr_title = pr["title"].as_str().unwrap_or("").to_string();
        ctx.pr_body = pr["body"].as_str().unwrap_or("").to_string();
    }
    ctx.comments = vec![("a person on the team".to_string(), text)];
    let j = judge(state, provider, &ctx, usage).await?;
    let lesson = j.lesson_to_store()?;
    info!(run_id = %msg.run_id, where_ = %j.where_, "A person said the fix belongs outside the code");
    enqueue(
        state,
        &LessonMessage {
            team_id: msg.team_id.clone(),
            installation_id: msg.installation_id,
            run_id: msg.run_id.clone(),
            repo_owner: msg.repo_owner.clone(),
            repo_name: msg.repo_name.clone(),
            pr_number: msg.pr_number,
            trigger: TRIGGER_COMMENT.to_string(),
            lesson: Some(lesson.to_string()),
            lesson_where: Some(j.where_.trim().to_string()).filter(|w| !w.is_empty()),
            attempts: 0,
        },
        0,
    )
    .await;
    if j.also_wants_code_change {
        // Feedback goes on to change the code; say what was learned first.
        let reply = ack_reply(&j);
        let _ = github
            .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, &reply)
            .await;
        return None;
    }
    // Feedback stops: the caller closes the PR with one note that says it.
    Some(j)
}

/// A person wrote on the ticket that the fix was made, or belongs, in
/// another system. The judge reads it with the run's ticket and findings;
/// when it holds a lesson, the lesson is queued for the repository's memory.
#[allow(clippy::too_many_arguments)]
pub async fn learn_from_ticket_comment(
    state: &WorkerState,
    provider: &ModelProvider,
    msg: &crate::models::JiraCommentMessage,
    usage: &mut TokenUsage,
) -> Option<Judgment> {
    let item = load_run(state, &msg.team_id, &msg.run_id).await?;
    let mut ctx = run_context(&item);
    ctx.comments = vec![(
        "a person on the team".to_string(),
        msg.comment.trim().to_string(),
    )];
    let j = judge(state, provider, &ctx, usage).await?;
    let lesson = j.lesson_to_store()?.to_string();
    if !msg.repo_owner.is_empty() && !msg.repo_name.is_empty() {
        enqueue(
            state,
            &LessonMessage {
                team_id: msg.team_id.clone(),
                installation_id: msg.installation_id,
                run_id: msg.run_id.clone(),
                repo_owner: msg.repo_owner.clone(),
                repo_name: msg.repo_name.clone(),
                pr_number: msg.pr_number,
                trigger: TRIGGER_COMMENT.to_string(),
                lesson: Some(lesson),
                lesson_where: Some(j.where_.trim().to_string()).filter(|w| !w.is_empty()),
                attempts: 0,
            },
            0,
        )
        .await;
    }
    Some(j)
}

/// The note CoderHelm closes its own pull request with when the fix belongs
/// outside the code: the cause and where the fix belongs, in a few lines.
pub fn closing_note(where_: &str, cause: &str) -> String {
    let place = if where_.trim().is_empty() {
        "outside this code".to_string()
    } else {
        format!("in {}", where_.trim())
    };
    format!(
        "Closing this pull request: the fix belongs {place}, not in this code. Changing the code \
         would override data that system owns.\n\n**Cause:** {}\n\nI've noted this for future \
         tickets on this repository and deleted the branch. Nothing else is needed here once the \
         change is made there.",
        cause.trim()
    )
}

/// What CoderHelm says back when told the fix was elsewhere.
pub fn ack_reply(j: &Judgment) -> String {
    let place = if j.where_.trim().is_empty() {
        "outside this code".to_string()
    } else {
        format!("in {}", j.where_.trim())
    };
    let mut out = format!(
        "Understood: the fix belongs {place}, not in this code. I've noted it for future tickets \
         on this repository:\n\n> {}\n",
        j.lesson.trim()
    );
    if !j.also_wants_code_change {
        out.push_str("\nI'm not changing the code for this.");
    }
    out
}

/// Sends a lesson job to the worker queue, after `delay` seconds.
async fn enqueue(state: &WorkerState, msg: &LessonMessage, delay: i32) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let body = match serde_json::to_value(msg) {
        Ok(mut v) => {
            if let Some(o) = v.as_object_mut() {
                o.insert("type".to_string(), serde_json::json!("lesson"));
            }
            v.to_string()
        }
        Err(_) => return false,
    };
    let mut send = state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body);
    if delay > 0 {
        send = send.delay_seconds(delay);
    }
    match send.send().await {
        Ok(_) => true,
        Err(e) => {
            warn!(error = %e, "Could not queue the lesson job");
            false
        }
    }
}

/// A lesson job: CoderHelm's PR was closed without merging (judge what
/// happened first), or feedback already judged a person's comment. Stores the
/// lesson in the repository's memory once per run.
pub async fn run(
    state: &WorkerState,
    msg: LessonMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(item) = load_run(state, &msg.team_id, &msg.run_id).await else {
        return Ok(());
    };
    if item.contains_key("lesson") {
        info!(run_id = %msg.run_id, "Lesson already recorded for this run");
        return Ok(());
    }

    let lesson = match msg.lesson.clone() {
        Some(l) => l,
        None => {
            let provider = ModelProvider::load_for_team(
                &state.dynamo,
                &state.config.settings_table_name,
                &msg.team_id,
            )
            .await?;
            let github = GitHubClient::new(
                &state.secrets.github_app_id,
                &state.secrets.github_private_key,
                msg.installation_id,
                &state.http,
            )?;
            let mut ctx = run_context(&item);
            ctx.closed_unmerged =
                msg.trigger == TRIGGER_PR_CLOSED || msg.trigger == TRIGGER_CLOSE_REASON;
            if let Ok(pr) = github
                .get_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
                .await
            {
                ctx.pr_title = pr["title"].as_str().unwrap_or("").to_string();
                ctx.pr_body = pr["body"].as_str().unwrap_or("").to_string();
            }
            if let Ok(c) = github
                .list_issue_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
                .await
            {
                ctx.comments = super::review_pr::human_comments(&c, 12);
            }
            // The GitHub issue the run came from, when it came from one.
            let issue_number = item
                .get("issue_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            if s(&item, "ticket_source") == "github" && issue_number > 0 {
                if let Ok(c) = github
                    .list_issue_comments(&msg.repo_owner, &msg.repo_name, issue_number)
                    .await
                {
                    ctx.comments
                        .extend(super::review_pr::human_comments(&c, 12));
                }
            }
            // Closed in CoderHelm with a reason picked: that is what people said.
            let run_reason = s(&item, "close_reason");
            if !run_reason.trim().is_empty() {
                ctx.comments.push((
                    "the person who closed it".to_string(),
                    format!("Closed because: {}", run_reason.trim()),
                ));
            }
            if msg.trigger == TRIGGER_PR_CLOSED && !has_reason(&ctx, &run_reason) {
                // Nobody said why: that is a signal of its own, and CoderHelm
                // asks once. The answer comes back as a close_reason lesson.
                ask_close_reason(state, &github, &msg).await;
                return Ok(());
            }
            if msg.trigger == TRIGGER_CLOSE_REASON {
                record_answer(state, &msg, &ctx).await;
            }
            let mut usage = TokenUsage::default();
            let judged = judge(state, &provider, &ctx, &mut usage).await;
            match judged.as_ref().and_then(Judgment::lesson_to_store) {
                Some(l) => l.to_string(),
                None => {
                    info!(run_id = %msg.run_id, "Closed PR: no lesson about where the fix belongs");
                    mark(state, &msg, "lesson_checked_at", None).await;
                    return Ok(());
                }
            }
        }
    };

    if std::env::var("MEMORY_ENABLED").unwrap_or_default() != "true" {
        return Ok(());
    }
    let Some(mut mem) =
        AgentMemory::open(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await
    else {
        // Another run holds the repository's memory: try again shortly, with
        // the lesson already judged.
        if msg.attempts < MAX_ATTEMPTS {
            let mut next = msg.clone();
            next.attempts += 1;
            next.lesson = Some(lesson);
            enqueue(state, &next, RETRY_DELAY_SECS).await;
        } else {
            warn!(run_id = %msg.run_id, "Repository memory stayed busy; lesson not stored");
        }
        return Ok(());
    };
    let origin = LearningOrigin {
        run_id: msg.run_id.clone(),
        ticket_id: s(&item, "ticket_id"),
        title: s(&item, "title"),
    };
    mem.store_lesson(&lesson, &origin).await;
    mem.close_and_upload(state).await?;
    mark(state, &msg, "lesson", Some(&lesson)).await;
    if let Some(w) = msg.lesson_where.as_deref().filter(|w| !w.trim().is_empty()) {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("run_id", attr_s(&msg.run_id))
            .update_expression("SET lesson_where = :w")
            .expression_attribute_values(":w", attr_s(w.trim()))
            .send()
            .await;
    }
    info!(run_id = %msg.run_id, trigger = %msg.trigger, "Stored a lesson on where the fix belongs");
    Ok(())
}

/// Records "closed, no reason" on the run and the team's counters, and asks
/// on the PR, once per run (a redelivered webhook asks nothing).
async fn ask_close_reason(state: &WorkerState, github: &GitHubClient, msg: &LessonMessage) {
    let now = chrono::Utc::now().to_rfc3339();
    let first = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .update_expression("SET close_signal = :sig, close_asked_at = :t")
        .condition_expression("attribute_exists(run_id) AND attribute_not_exists(close_asked_at)")
        .expression_attribute_values(":sig", attr_s(SIGNAL_NO_REASON))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await
        .is_ok();
    if !first {
        return;
    }
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in [month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("period", attr_s(period))
            .update_expression("ADD prs_closed_no_reason :one")
            .expression_attribute_values(":one", crate::passes::attr_n(1))
            .send()
            .await;
    }
    info!(run_id = %msg.run_id, pr = msg.pr_number, "PR closed without a reason: asking once");
    if let Err(e) = github
        .create_issue_comment(
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &close_reason_question(),
        )
        .await
    {
        warn!(run_id = %msg.run_id, error = %e, "Could not ask why the PR was closed");
    }
}

/// The answer to the one question: kept on the run (the planner reads it
/// with the ticket's history) and the signal moved to "closed with reason".
async fn record_answer(state: &WorkerState, msg: &LessonMessage, ctx: &LessonContext) {
    let Some((_, answer)) = ctx
        .comments
        .iter()
        .rev()
        .find(|(_, t)| !t.trim().is_empty())
    else {
        return;
    };
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .update_expression(
            "SET close_signal = :sig, close_reason_answer = :a, close_answered_at = :t",
        )
        .condition_expression("attribute_exists(close_asked_at)")
        .expression_attribute_values(":sig", attr_s(SIGNAL_WITH_REASON))
        .expression_attribute_values(":a", attr_s(common::truncate_str(answer.trim(), 1000)))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;
}

async fn mark(state: &WorkerState, msg: &LessonMessage, field: &str, value: Option<&str>) {
    let now = chrono::Utc::now().to_rfc3339();
    let mut up = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .expression_attribute_names("#f", field);
    up = match value {
        Some(v) => up
            .update_expression("SET #f = :v, lesson_at = :t")
            .expression_attribute_values(":v", attr_s(v))
            .expression_attribute_values(":t", attr_s(&now)),
        None => up
            .update_expression("SET #f = :t")
            .expression_attribute_values(":t", attr_s(&now)),
    };
    let _ = up.send().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A close with nothing said is asked about once; anything people said
    /// (on the PR, the ticket, or the close dialog) is the reason.
    #[test]
    fn a_close_without_a_reason_is_asked_about() {
        let mut ctx = LessonContext::default();
        assert!(!has_reason(&ctx, ""));
        assert!(has_reason(&ctx, "not needed"));
        ctx.comments.push(("pat".into(), "  ".into()));
        assert!(!has_reason(&ctx, ""));
        ctx.comments.push(("pat".into(), "Wrong approach".into()));
        assert!(has_reason(&ctx, ""));
        let q = close_reason_question();
        assert!(q.starts_with(CLOSE_ASK_MARKER));
        assert!(q.contains("I won't ask again"));
        assert!(q.len() < 400);
    }

    fn closed_case() -> LessonContext {
        let d =
            crate::passes::fix_location::parse(crate::passes::fix_location::fixture::PLANNER_REPLY)
                .unwrap();
        LessonContext {
            ticket_title: "Saturday class missing from the site".into(),
            ticket_body: "Program 12345 doesn't show.".into(),
            plan_findings: crate::passes::fix_location::reviewer_context(&d),
            pr_title: "Force program 12345 active".into(),
            pr_body: "Adds FORCE_ACTIVE_PROGRAM_IDS = {\"12345\"}".into(),
            comments: vec![(
                "owner".into(),
                "For this the fix was in the booking system.".into(),
            )],
            closed_unmerged: true,
        }
    }

    #[test]
    fn the_judge_sees_the_ticket_the_findings_the_pr_and_the_people() {
        let p = prompt(&closed_case());
        assert!(p.contains("Saturday class missing from the site"));
        assert!(p.contains("Data shows: Term 678 has available_on_web = false"));
        assert!(p.contains("Force program 12345 active"));
        assert!(p.contains("closed without being merged"));
        assert!(p.contains("- owner: For this the fix was in the booking system."));
    }

    #[test]
    fn only_a_fix_that_was_elsewhere_is_remembered() {
        let j = parse_judgment(
            "```json\n{\"fix_was_elsewhere\": true, \"where\": \"the booking system\", \
             \"lesson\": \"Classes missing from the site are often because the semester is not \
             available on web in the booking system; check that first.\", \
             \"also_wants_code_change\": false}\n```",
        )
        .unwrap();
        assert!(j.lesson_to_store().unwrap().starts_with("Classes missing"));
        let reply = ack_reply(&j);
        assert!(reply.contains("the fix belongs in the booking system"));
        assert!(reply.contains("> Classes missing from the site"));
        assert!(reply.contains("I'm not changing the code"));

        let other =
            parse_judgment("{\"fix_was_elsewhere\": false, \"lesson\": \"Use smaller PRs.\"}")
                .unwrap();
        assert_eq!(other.lesson_to_store(), None);
        assert_eq!(parse_judgment("not json"), None);
    }

    #[test]
    fn the_closing_note_says_the_cause_and_where_the_fix_belongs() {
        let j = Judgment {
            fix_was_elsewhere: true,
            where_: "the booking system".into(),
            lesson: "Classes missing from the site are often because their term is not on the web."
                .into(),
            also_wants_code_change: false,
        };
        let n = closing_note(&j.where_, &j.lesson);
        assert!(n.starts_with("Closing this pull request: the fix belongs in the booking system"));
        assert!(n.contains("**Cause:** Classes missing"));
        assert!(n.contains("deleted the branch"));
    }

    #[test]
    fn the_lesson_message_round_trips_on_the_queue() {
        let m = LessonMessage {
            team_id: "TEAM#1".into(),
            installation_id: 7,
            run_id: "run-1".into(),
            repo_owner: "acme".into(),
            repo_name: "data-sync".into(),
            pr_number: 42,
            trigger: TRIGGER_PR_CLOSED.into(),
            lesson: None,
            lesson_where: None,
            attempts: 0,
        };
        let mut v = serde_json::to_value(&m).unwrap();
        v["type"] = serde_json::json!("lesson");
        let back: crate::models::WorkerMessage = serde_json::from_value(v).unwrap();
        assert!(matches!(back, crate::models::WorkerMessage::Lesson(l) if l.pr_number == 42));
    }
}
