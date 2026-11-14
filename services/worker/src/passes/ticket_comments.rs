//! The worker's side of ticket comments (see `common::ticket_comments`):
//! what people wrote on a ticket is read where the run can still use it,
//! and nothing a person wrote is lost when the run moves on.
//!
//! A run's open comments are read:
//!
//! - when the run starts and at each step boundary ([`at_boundary`]),
//!   before it spends more work: answers and instructions go into the plan
//!   while it is still being made, and are kept for a round on the pull
//!   request once the code is being built from the final plan;
//! - when a step ends ([`after_step`]): with the pull request open, the
//!   comments kept for it become one feedback round; a run that ended
//!   waiting on a person or failed starts the ticket again with them; a
//!   run a person stopped keeps them on the ticket for the next run;
//! - as soon as one arrives ([`on_comment`]), so a comment that says to
//!   stop ends the run now and not at its next step, and a comment that
//!   reached a run a moment after it ended is still acted on.
//!
//! Every read takes all open comments together (one model call for a
//! burst), and a comment is claimed in the store before anything is done
//! with it, so two readers never act on it twice.

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{
    FeedbackMessage, JiraCommentMessage, TicketMessage, TicketSource, TokenUsage, WorkerMessage,
};
use crate::passes::attr_s;
use crate::WorkerState;
use common::run_questions::Answer;
use common::ticket_comments::{self as tc, Action, Comment, Settled, Source, Stage, Store};
use common::ticket_state::{Intent, Note};
use tracing::{info, warn};

/// What a boundary read gives the run.
#[derive(Debug, Default)]
pub struct Read {
    /// Answers and instructions for the plan being made: the planner reads
    /// them word for word.
    pub folded: Vec<Answer>,
    /// A person said to stop: the run ends now.
    pub stopped: bool,
}

/// The ticket a read is for, and how to reach it.
struct Ticket<'a> {
    team_id: &'a str,
    ticket_id: &'a str,
    run_id: &'a str,
    installation_id: u64,
}

fn store(state: &WorkerState) -> tc::Dynamo<'_> {
    tc::Dynamo {
        client: &state.dynamo,
        table: &state.config.settings_table_name,
    }
}

/// What the run's record says about where it is.
#[derive(Debug, Default, Clone)]
struct RunFacts {
    status: String,
    current_pass: String,
    pr_number: u64,
    pr_outcome: String,
    repo: String,
    title: String,
    issue_number: u64,
    updated_at: String,
    /// The questions the run told the ticket about and kept building
    /// without.
    questions: Vec<String>,
}

impl RunFacts {
    fn pr_open(&self) -> bool {
        self.pr_number > 0 && !matches!(self.pr_outcome.as_str(), "closed" | "merged")
    }

    fn pr_link(&self) -> Option<String> {
        (self.pr_number > 0 && !self.repo.is_empty())
            .then(|| format!("https://github.com/{}/pull/{}", self.repo, self.pr_number))
    }
}

async fn run_facts(state: &WorkerState, team_id: &str, run_id: &str) -> Option<RunFacts> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| warn!(run_id, error = %e, "Could not read the run for its ticket comments"))
        .ok()?
        .item?;
    let text = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let number = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let mut questions: Vec<String> =
        serde_json::from_str(&text(tc::FIELD_ASKED)).unwrap_or_default();
    // A run that stopped to ask keeps its questions as asked (read only).
    if let Ok(asked) =
        serde_json::from_str::<common::run_questions::Asked>(&text(tc::FIELD_STOPPED_TO_ASK))
    {
        for q in asked.questions {
            if !questions.contains(&q.question) {
                questions.push(q.question);
            }
        }
    }
    Some(RunFacts {
        status: text("status"),
        current_pass: text("current_pass"),
        pr_number: number("pr_number"),
        pr_outcome: text("pr_outcome"),
        repo: text("repo"),
        title: text("title"),
        issue_number: number("issue_number"),
        updated_at: text("updated_at"),
        questions,
    })
}

/// Whether a run still marked as working has gone quiet for longer than a
/// working run can (the bounds the trigger gate uses).
pub fn gone_quiet(status: &str, updated_at: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
    let hours = chrono::DateTime::parse_from_rfc3339(updated_at)
        .map(|t| now.signed_duration_since(t).num_hours())
        .unwrap_or(0);
    match status {
        "running" | "queued" => hours >= 2,
        "awaiting_ci" => hours >= 26,
        _ => false,
    }
}

/// Keeps the questions the run told its ticket about on the run record, so
/// a comment that answers one is read as that answer.
pub async fn keep_asked(state: &WorkerState, team_id: &str, run_id: &str, questions: &[String]) {
    let asked: Vec<&str> = questions
        .iter()
        .map(|q| q.trim())
        .filter(|q| !q.is_empty())
        .collect();
    if asked.is_empty() {
        return;
    }
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #q = :q")
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_names("#q", tc::FIELD_ASKED)
        .expression_attribute_values(
            ":q",
            attr_s(&serde_json::to_string(&asked).unwrap_or_default()),
        )
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not keep the run's questions for its ticket comments");
    }
}

/// The model's reading of the unread comments, in order; every one reads
/// as an instruction (never a stop) when the model can't be reached.
async fn classify(
    state: &WorkerState,
    team_id: &str,
    title: &str,
    questions: &[String],
    unread: &[Comment],
    usage: &mut TokenUsage,
) -> Vec<tc::Reading> {
    let provider = match ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!(team_id, error = %e, "No model to read the ticket comments; read as instructions");
            return Vec::new();
        }
    };
    match crate::agent::anthropic::converse_json(
        &provider.client(),
        provider.judgment_model_id(),
        tc::SYSTEM,
        &tc::prompt(title, questions, unread),
        &tc::schema(),
        true,
        common::stop_judge::JUDGMENT_EFFORT,
        4096,
        usage,
    )
    .await
    {
        Ok(reply) => tc::readings(&reply, questions, unread.len()),
        Err(e) => {
            warn!(team_id, error = %e, "Could not read the ticket comments; read as instructions");
            Vec::new()
        }
    }
}

/// What a settled read asks of the worker, worked out before anything is
/// sent.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Effects {
    /// For the plan being made.
    pub folded: Vec<Answer>,
    /// The feedback round on the open pull request, when there is one.
    pub round: Option<String>,
    /// The direction the ticket starts again with, when it does.
    pub restart: Option<(String, Vec<Answer>)>,
    /// The person who said to stop, when one did.
    pub stop_by: Option<String>,
    /// Kept on the ticket's record for later runs.
    pub ticket_notes: Vec<Note>,
    /// One line per comment for the run page.
    pub run_notes: Vec<String>,
    /// The one comment CoderHelm posts on the ticket, when it has
    /// something to say.
    pub reply: Option<String>,
}

/// The work a settled read asks for.
pub fn effects(settled: &Settled, pr: Option<&str>) -> Effects {
    let round = settled.with(Action::Round);
    let restart = settled.with(Action::Restart);
    let kept_for_later = |a: &Action| {
        matches!(
            a,
            Action::Fold | Action::Round | Action::Restart | Action::Keep
        )
    };
    Effects {
        folded: settled
            .with(Action::Fold)
            .iter()
            .map(tc::as_answer)
            .collect(),
        round: (!round.is_empty()).then(|| tc::round_body(&round)),
        restart: (!restart.is_empty()).then(|| {
            (
                tc::direction(&restart),
                restart.iter().map(tc::as_answer).collect(),
            )
        }),
        stop_by: settled.with(Action::Stop).first().map(|c| c.author.clone()),
        ticket_notes: settled
            .done
            .iter()
            .filter(|(a, _)| kept_for_later(a))
            .map(|(_, c)| Note {
                author: c.author.clone(),
                text: common::truncate_str(c.text.trim(), 2000).to_string(),
                intent: Intent::NewRequirement,
                at: c.at.clone(),
            })
            .collect(),
        run_notes: settled
            .done
            .iter()
            .map(|(_, c)| c.outcome.clone())
            .collect(),
        reply: tc::ticket_reply(&settled.done, pr),
    }
}

/// Reads the ticket's open comments for the run and does what each asks.
/// `in_step`: read by the run's own step. `restart`: the ticket's message,
/// when this reader may start the ticket again.
async fn read(
    state: &WorkerState,
    t: &Ticket<'_>,
    in_step: bool,
    restart: Option<&TicketMessage>,
    usage: &mut TokenUsage,
) -> Read {
    let store = store(state);
    let mut open = match store.of_ticket(t.team_id, t.ticket_id).await {
        Ok(all) => tc::open(all),
        Err(e) => {
            warn!(ticket_id = t.ticket_id, error = %e, "Could not read the ticket's comments; they stay open");
            return Read::default();
        }
    };
    if open.is_empty() {
        return Read::default();
    }
    let Some(facts) = run_facts(state, t.team_id, t.run_id).await else {
        return Read::default();
    };
    let stale = gone_quiet(&facts.status, &facts.updated_at, chrono::Utc::now());
    let mut stage = tc::stage_of(
        &facts.status,
        &facts.current_pass,
        facts.pr_open(),
        in_step,
        stale,
    );
    // Only the run's own step may start the ticket again: any other reader
    // keeps the comments on the ticket for the next run.
    if let (
        Stage::Finished {
            pr_open: false,
            waiting: true,
        },
        None,
    ) = (stage, restart)
    {
        stage = Stage::Finished {
            pr_open: false,
            waiting: false,
        };
    }

    let unread: Vec<Comment> = open.iter().filter(|c| c.class.is_none()).cloned().collect();
    if !unread.is_empty() {
        let readings = classify(
            state,
            t.team_id,
            &facts.title,
            &facts.questions,
            &unread,
            usage,
        )
        .await;
        tc::keep_readings(&store, t.team_id, &mut open, &readings).await;
    }

    let pr = facts.pr_link();
    let mut settled =
        tc::settle_open(&store, t.team_id, t.run_id, &open, stage, pr.as_deref()).await;
    if settled.done.is_empty() {
        return Read::default();
    }
    info!(
        run_id = t.run_id,
        ticket_id = t.ticket_id,
        stage = ?stage,
        comments = settled.done.len(),
        "Read the ticket's comments"
    );
    // The work is sent first. What couldn't be sent is put back as open
    // (whatever reads the ticket next takes it up) and nothing is said
    // about it.
    let planned = effects(&settled, pr.as_deref());
    if let Some(body) = &planned.round {
        if !send_round(state, t, &facts, body).await {
            reopen(&store, t.team_id, &settled.with(Action::Round)).await;
            settled.done.retain(|(a, _)| *a != Action::Round);
        }
    }
    if let Some((direction, answers)) = &planned.restart {
        let started = match restart {
            Some(msg) => start_again(state, msg, direction, answers).await,
            None => false,
        };
        if !started {
            reopen(&store, t.team_id, &settled.with(Action::Restart)).await;
            settled.done.retain(|(a, _)| *a != Action::Restart);
        }
    }
    let fx = effects(&settled, pr.as_deref());
    if let Some(by) = &fx.stop_by {
        stop_run(state, t, by).await;
    }
    for line in &fx.run_notes {
        super::add_progress_note(state, t.team_id, t.run_id, line).await;
    }
    super::jira_comment::record_notes(state, t.team_id, t.ticket_id, fx.ticket_notes.clone(), None)
        .await;
    if let Some(reply) = &fx.reply {
        say_on_ticket(state, t, &facts, &open, reply).await;
    }
    // An answer to a question CoderHelm asked marks that question answered
    // in the comment that asked it.
    for (_, c) in settled.done.iter().filter(|(a, c)| {
        matches!(a, Action::Fold | Action::Round | Action::Defer) && !c.question.trim().is_empty()
    }) {
        mark_answered(state, t, &facts, c).await;
    }
    Read {
        folded: fx.folded,
        stopped: fx.stop_by.is_some(),
    }
}

/// Puts comments back as open when the work they asked for couldn't be
/// sent: whatever reads the ticket next takes them up.
async fn reopen(store: &dyn Store, team_id: &str, comments: &[Comment]) {
    for c in comments {
        let mut back = c.clone();
        back.state = tc::State::Pending;
        back.outcome.clear();
        back.handled_by.clear();
        back.handled_at.clear();
        if let Err(e) = store.settle(team_id, &back, c.state).await {
            warn!(ticket_id = %c.ticket_id, error = %e, "Could not reopen a ticket comment");
        }
    }
}

/// Ends the run because a person said to stop, the way a cancel from the
/// dashboard does: the run's next boundary sees it and stops.
async fn stop_run(state: &WorkerState, t: &Ticket<'_>, by: &str) {
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(t.team_id))
        .key("run_id", attr_s(t.run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, updated_at = :t, cancelled_by = :cb, cancel_reason = :cr",
        )
        .condition_expression("#s IN (:running, :queued, :awaiting)")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s(common::run_status::CANCELLED))
        .expression_attribute_values(
            ":sri",
            attr_s(&format!("{}#{}", common::run_status::CANCELLED, t.run_id)),
        )
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":cb", attr_s(&format!("{by} (comment on the ticket)")))
        .expression_attribute_values(":cr", attr_s("cancelled_by_user"))
        .expression_attribute_values(":running", attr_s("running"))
        .expression_attribute_values(":queued", attr_s("queued"))
        .expression_attribute_values(":awaiting", attr_s("awaiting_ci"))
        .send()
        .await;
    match res {
        Ok(_) => info!(
            run_id = t.run_id,
            "Run stopped: a person said to stop on the ticket"
        ),
        Err(e) => {
            warn!(run_id = t.run_id, error = %e, "Could not stop the run for the ticket comment (it may have ended already)")
        }
    }
}

/// Sends the feedback round for the comments onto the run's open pull
/// request. `false` when it couldn't be sent.
async fn send_round(state: &WorkerState, t: &Ticket<'_>, facts: &RunFacts, body: &str) -> bool {
    let Some((owner, name)) = facts.repo.split_once('/') else {
        warn!(
            run_id = t.run_id,
            "The run names no repository for the round"
        );
        return false;
    };
    let message = WorkerMessage::Feedback(FeedbackMessage {
        team_id: t.team_id.to_string(),
        installation_id: t.installation_id,
        run_id: t.run_id.to_string(),
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number: facts.pr_number,
        review_id: 0,
        review_body: body.to_string(),
        comments: vec![],
        // Written on the ticket, not the pull request: nobody to address
        // there. The round says what changed in the pull request itself.
        trigger_author: String::new(),
        ..Default::default()
    });
    send(state, &state.config.feedback_queue_url, &message).await
}

/// Starts the ticket again with the comments as a person's direction (a
/// run that ended waiting on a person, or failed). `false` when another
/// run is already starting on the ticket or it couldn't be sent.
async fn start_again(
    state: &WorkerState,
    msg: &TicketMessage,
    direction: &str,
    answers: &[Answer],
) -> bool {
    if !super::acquire_ticket_lock(state, &msg.team_id, &msg.ticket_id).await {
        info!(ticket_id = %msg.ticket_id, "Ticket lock held — the run starting on the ticket reads the comments");
        return false;
    }
    let message = WorkerMessage::Ticket(fresh_run(msg, direction, answers));
    send(state, &state.config.ticket_queue_url, &message).await
}

/// The ticket's message for a new run that takes up the comments: the
/// same ticket, none of the ended run's own state.
pub fn fresh_run(msg: &TicketMessage, direction: &str, answers: &[Answer]) -> TicketMessage {
    let mut all = msg.answers.clone();
    common::run_questions::merge(&mut all, answers);
    TicketMessage {
        team_id: msg.team_id.clone(),
        installation_id: msg.installation_id,
        source: msg.source.clone(),
        ticket_id: msg.ticket_id.clone(),
        title: msg.title.clone(),
        body: msg.body.clone(),
        repo_owner: msg.repo_owner.clone(),
        repo_name: msg.repo_name.clone(),
        issue_number: msg.issue_number,
        sender: msg.sender.clone(),
        requester: msg.requester.clone(),
        base_branch: msg.base_branch.clone(),
        image_attachments: msg.image_attachments.clone(),
        ticket_type: msg.ticket_type.clone(),
        person_direction: Some(direction.to_string()),
        answers: all,
        ..Default::default()
    }
}

async fn send(state: &WorkerState, queue: &str, message: &WorkerMessage) -> bool {
    if queue.is_empty() {
        warn!("No queue configured for the ticket comments' work");
        return false;
    }
    let body = match serde_json::to_string(message) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "Could not serialize the ticket comments' work");
            return false;
        }
    };
    match state
        .sqs
        .send_message()
        .queue_url(queue)
        .message_body(body)
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(error = %aws_sdk_sqs::error::DisplayErrorContext(&e), "Could not send the ticket comments' work");
            false
        }
    }
}

/// CoderHelm's one comment on the ticket about what it did with the
/// comments it read. Best effort.
async fn say_on_ticket(
    state: &WorkerState,
    t: &Ticket<'_>,
    facts: &RunFacts,
    open: &[Comment],
    reply: &str,
) {
    let source = open.first().map(|c| c.source).unwrap_or_default();
    match source {
        Source::Jira => {
            if let Err(e) = crate::passes::conversation::jira(
                state,
                t.team_id,
                t.ticket_id,
                common::conversation::Say::reply_all("ticket-comments", reply),
                "info",
                "Comments read",
            )
            .await
            .map(|_| ())
            {
                warn!(ticket_id = t.ticket_id, error = %e, "Could not say on the ticket what was done with its comments");
            }
        }
        Source::Github => {
            let Some((owner, name)) = facts.repo.split_once('/') else {
                return;
            };
            if facts.issue_number == 0 {
                return;
            }
            let github = match GitHubClient::new(
                &state.secrets.github_app_id,
                &state.secrets.github_private_key,
                t.installation_id,
                &state.http,
            ) {
                Ok(g) => g,
                Err(e) => {
                    warn!(ticket_id = t.ticket_id, error = %e, "No GitHub client to reply on the issue");
                    return;
                }
            };
            if let Err(e) = crate::passes::conversation::github(
                &github,
                owner,
                name,
                facts.issue_number,
                common::conversation::Say::reply_all("ticket-comments", reply),
            )
            .await
            {
                warn!(ticket_id = t.ticket_id, error = %e, "Could not say on the issue what was done with its comments");
            }
        }
    }
}

/// Marks `c`'s question answered in CoderHelm's comment that asked it.
/// Best effort.
async fn mark_answered(state: &WorkerState, t: &Ticket<'_>, facts: &RunFacts, c: &Comment) {
    let res = match c.source {
        Source::Jira => {
            crate::passes::conversation::mark_answered_jira(
                state,
                t.team_id,
                t.ticket_id,
                &c.question,
                &c.author,
                &c.text,
            )
            .await
        }
        Source::Github => {
            let Some((owner, name)) = facts.repo.split_once('/') else {
                return;
            };
            if facts.issue_number == 0 {
                return;
            }
            match GitHubClient::new(
                &state.secrets.github_app_id,
                &state.secrets.github_private_key,
                t.installation_id,
                &state.http,
            ) {
                Ok(gh) => {
                    crate::passes::conversation::mark_answered_github(
                        &gh,
                        owner,
                        name,
                        facts.issue_number,
                        &c.question,
                        &c.author,
                        &c.text,
                    )
                    .await
                }
                Err(e) => Err(e.to_string().into()),
            }
        }
    };
    if let Err(e) = res {
        warn!(ticket_id = t.ticket_id, error = %e, "Could not mark the question answered on the ticket");
    }
}

/// The run's own step reads its ticket's open comments, before it spends
/// more work.
pub async fn at_boundary(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    usage: &mut TokenUsage,
) -> Read {
    if !reads_comments(msg) {
        return Read::default();
    }
    read(
        state,
        &Ticket {
            team_id: &msg.team_id,
            ticket_id: &msg.ticket_id,
            run_id,
            installation_id: msg.installation_id,
        },
        true,
        None,
        usage,
    )
    .await
}

/// A step ended: what the run kept for its pull request becomes a round
/// on it, and a run that ended some other way hands its open comments to
/// what comes next.
pub async fn after_step(state: &WorkerState, msg: &TicketMessage, run_id: &str) {
    if !reads_comments(msg) {
        return;
    }
    let mut usage = TokenUsage::default();
    read(
        state,
        &Ticket {
            team_id: &msg.team_id,
            ticket_id: &msg.ticket_id,
            run_id,
            installation_id: msg.installation_id,
        },
        false,
        Some(msg),
        &mut usage,
    )
    .await;
}

/// A comment just arrived for the ticket's run (or for a run that has
/// ended): read the ticket's open comments now.
pub async fn on_comment(state: &WorkerState, msg: &JiraCommentMessage) {
    if msg.run_id.is_empty() || msg.ticket_id.is_empty() {
        return;
    }
    let mut usage = TokenUsage::default();
    read(
        state,
        &Ticket {
            team_id: &msg.team_id,
            ticket_id: &msg.ticket_id,
            run_id: &msg.run_id,
            installation_id: msg.installation_id,
        },
        false,
        None,
        &mut usage,
    )
    .await;
}

/// A round on the run's pull request ended (feedback, a CI or review fix):
/// comments written on the ticket while it worked are read now.
pub async fn after_round(state: &WorkerState, team_id: &str, run_id: &str) {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("ticket_id, installation_id")
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let Some(item) = item else {
        return;
    };
    let ticket_id = item
        .get("ticket_id")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    if ticket_id.is_empty() {
        return;
    }
    let installation_id = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let mut usage = TokenUsage::default();
    read(
        state,
        &Ticket {
            team_id,
            ticket_id: &ticket_id,
            run_id,
            installation_id,
        },
        false,
        None,
        &mut usage,
    )
    .await;
}

/// Tickets people comment on: a Jira ticket or a GitHub issue. An alert
/// has no thread.
fn reads_comments(msg: &TicketMessage) -> bool {
    !msg.ticket_id.is_empty() && matches!(msg.source, TicketSource::Jira | TicketSource::Github)
}

/// What people already wrote on a GitHub issue, kept when a run starts on
/// it so the run reads it before it plans (a Jira ticket's comments come
/// with its event). Comments already kept are left as they are.
pub async fn keep_existing_github(state: &WorkerState, msg: &TicketMessage, github: &GitHubClient) {
    if !matches!(msg.source, TicketSource::Github)
        || msg.issue_number == 0
        || msg.same_run()
        || msg.repo_owner.is_empty()
    {
        return;
    }
    let comments = match github
        .list_issue_comments(&msg.repo_owner, &msg.repo_name, msg.issue_number)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            warn!(ticket_id = %msg.ticket_id, error = %e, "Could not read the issue's comments");
            return;
        }
    };
    let store = store(state);
    for c in existing_github(&msg.ticket_id, &comments) {
        if let Err(e) = store.record(&msg.team_id, &c).await {
            warn!(ticket_id = %msg.ticket_id, error = %e, "Could not keep an issue comment");
        }
    }
}

/// A fresh run on a Jira ticket says on its page when the event that
/// started it carried no comments (an older Jira app, or a plain Jira
/// webhook; CoderHelm cannot read them another way on either): what
/// people wrote before the run was not read, and why for that path.
pub async fn note_unread_earlier(state: &WorkerState, msg: &TicketMessage, run_id: &str) {
    if !matches!(msg.source, TicketSource::Jira) || msg.same_run() || msg.ticket_id.is_empty() {
        return;
    }
    let marker = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&common::ticket_state::sk(&msg.ticket_id)))
        .projection_expression("#f")
        .expression_attribute_names("#f", tc::FIELD_EVENT_COMMENTS)
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| {
            i.get(tc::FIELD_EVENT_COMMENTS)
                .and_then(|v| v.as_s().ok())
                .cloned()
        });
    if let Some(note) = tc::unread_earlier_note(marker.as_deref()) {
        info!(run_id, ticket_id = %msg.ticket_id, "The event that started the run carried no ticket comments");
        super::add_progress_note(state, &msg.team_id, run_id, note).await;
    }
}

/// Most comments read from an issue when a run starts on it (the newest).
pub const MAX_EXISTING: usize = 20;

/// The people's comments on an issue as kept for a run that starts on it:
/// context (no run was working when they were written), newest
/// [`MAX_EXISTING`], oldest first.
pub fn existing_github(ticket_id: &str, comments: &[serde_json::Value]) -> Vec<Comment> {
    let people: Vec<Comment> = comments
        .iter()
        .filter_map(|c| {
            let author = c["user"]["login"].as_str().unwrap_or("");
            let kind = c["user"]["type"].as_str().unwrap_or("");
            let text = c["body"].as_str().unwrap_or("");
            let id = c["id"].as_u64()?;
            if text.trim().is_empty() || !tc::is_person(Source::Github, author, kind, text) {
                return None;
            }
            Some(Comment {
                ticket_id: ticket_id.to_string(),
                source: Source::Github,
                comment_id: id.to_string(),
                author: author.to_string(),
                text: common::truncate_str(text.trim(), tc::MAX_TEXT_CHARS).to_string(),
                at: c["created_at"].as_str().unwrap_or("").to_string(),
                ..Default::default()
            })
        })
        .collect();
    let skip = people.len().saturating_sub(MAX_EXISTING);
    people.into_iter().skip(skip).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::ticket_comments::{Class, Memory, State};
    use serde_json::json;

    fn comment(id: &str, text: &str, class: Class) -> Comment {
        Comment {
            ticket_id: "ABC-1".into(),
            source: Source::Jira,
            comment_id: id.into(),
            author: "Pat Doe".into(),
            text: text.into(),
            at: format!("2026-01-01T00:00:0{id}Z"),
            run_id: "01RUN".into(),
            class: Some(class),
            question: if class == Class::Answer {
                "Which environments get the flag?".into()
            } else {
                String::new()
            },
            ..Default::default()
        }
    }

    async fn settled_at(stage: Stage, comments: &[Comment]) -> (Memory, Settled) {
        let store = Memory::default();
        for c in comments {
            store.record("TEAM#1", c).await.unwrap();
        }
        let open = tc::open(store.all("TEAM#1", "ABC-1"));
        let s = tc::settle_open(&store, "TEAM#1", "01RUN", &open, stage, Some("PR")).await;
        (store, s)
    }

    fn burst() -> Vec<Comment> {
        vec![
            comment("1", "Production only.", Class::Answer),
            comment("2", "Also rename the flag.", Class::Instruction),
            comment("3", "Thanks all", Class::NotForCoderhelm),
        ]
    }

    #[tokio::test]
    async fn while_planning_answers_and_instructions_go_into_the_plan() {
        let (store, s) = settled_at(Stage::Planning, &burst()).await;
        let fx = effects(&s, None);
        assert_eq!(fx.folded.len(), 2);
        assert_eq!(fx.folded[0].question, "Which environments get the flag?");
        assert_eq!(fx.folded[0].answer, "Production only.");
        assert_eq!(fx.folded[1].question, tc::COMMENT_QUESTION);
        // The planner reads them word for word, as settled.
        let note = common::run_questions::answers_note(&fx.folded);
        assert!(note.contains("Answer from Pat Doe: Production only."));
        assert!(note.contains("Also rename the flag."));
        // No round, no restart, no stop, nothing more said on the ticket.
        assert_eq!(fx.round, None);
        assert_eq!(fx.restart, None);
        assert_eq!(fx.stop_by, None);
        assert_eq!(fx.reply, None);
        // Kept on the ticket for later runs; the chatter isn't.
        assert_eq!(fx.ticket_notes.len(), 2);
        // The run page says what was done with each of the three.
        assert_eq!(fx.run_notes.len(), 3);
        assert!(fx.run_notes[0].contains("folded into the plan"));
        assert!(fx.run_notes[2].contains("not for CoderHelm: left as it is"));
        assert!(tc::open(store.all("TEAM#1", "ABC-1")).is_empty());
    }

    #[tokio::test]
    async fn while_building_or_reviewing_they_wait_for_the_pull_request() {
        for stage in [Stage::Building, Stage::Reviewing] {
            let (store, s) = settled_at(stage, &burst()).await;
            let fx = effects(&s, None);
            assert!(fx.folded.is_empty(), "the plan is final at {stage:?}");
            assert_eq!(fx.round, None, "no round before the step ends");
            assert_eq!(fx.reply, None);
            assert!(fx.run_notes[0].contains("applied as a round on the pull request"));
            let open = tc::open(store.all("TEAM#1", "ABC-1"));
            assert_eq!(open.len(), 2);
            assert!(open.iter().all(|c| c.state == State::Deferred));

            // A later boundary of the same step leaves them be.
            let again = tc::settle_open(&store, "TEAM#1", "01RUN", &open, stage, None).await;
            assert!(again.done.is_empty());

            // The step ends with the pull request open: one round for both,
            // said once on the ticket.
            let done = Stage::Finished {
                pr_open: true,
                waiting: false,
            };
            let end = tc::settle_open(&store, "TEAM#1", "01RUN", &open, done, Some("PR#12")).await;
            let fx = effects(&end, Some("PR#12"));
            let round = fx.round.expect("a round on the open pull request");
            assert!(round.contains("Answer: Production only."));
            assert!(round.contains("Also rename the flag."));
            let reply = fx.reply.expect("said on the ticket");
            assert!(reply.contains("on PR#12"));
            assert!(tc::open(store.all("TEAM#1", "ABC-1")).is_empty());
        }
    }

    #[tokio::test]
    async fn between_steps_they_stay_for_the_next_step() {
        let (store, s) = settled_at(Stage::BetweenSteps, &burst()).await;
        // Only the chatter is settled (left alone); the rest wait.
        assert_eq!(s.done.len(), 1);
        assert_eq!(s.done[0].0, Action::Leave);
        let open = tc::open(store.all("TEAM#1", "ABC-1"));
        assert_eq!(open.len(), 2);
        assert!(open.iter().all(|c| c.state == State::Pending));
        // The fresh step reads them at its first boundary.
        let next = tc::settle_open(&store, "TEAM#1", "01RUN", &open, Stage::Building, None).await;
        assert_eq!(next.with(Action::Defer).len(), 2);
    }

    #[tokio::test]
    async fn a_finished_run_gets_a_round_and_a_waiting_one_starts_again() {
        let done = Stage::Finished {
            pr_open: true,
            waiting: false,
        };
        let (_, s) = settled_at(done, &burst()).await;
        let fx = effects(&s, Some("PR#12"));
        assert!(fx.round.is_some());
        assert_eq!(fx.restart, None);

        // The run ended waiting on a person (or failed) with no pull
        // request: the ticket starts again with what was written.
        let waiting = Stage::Finished {
            pr_open: false,
            waiting: true,
        };
        let (_, s) = settled_at(waiting, &burst()).await;
        let fx = effects(&s, None);
        assert_eq!(fx.round, None);
        let (direction, answers) = fx.restart.expect("the ticket starts again");
        assert!(direction.contains("Pat Doe: Production only."));
        assert_eq!(answers.len(), 2);
        let msg = TicketMessage {
            team_id: "TEAM#1".into(),
            ticket_id: "ABC-1".into(),
            title: "Add the flag".into(),
            body: "b".into(),
            source: TicketSource::Jira,
            continuation: 4,
            continuation_run_id: Some("01RUN".into()),
            resume_from: Some("01OLD".into()),
            ..Default::default()
        };
        let fresh = fresh_run(&msg, &direction, &answers);
        assert_eq!(fresh.ticket_id, "ABC-1");
        assert_eq!(fresh.continuation, 0, "a new run, not the ended one");
        assert_eq!(fresh.continuation_run_id, None);
        assert_eq!(fresh.resume_from, None);
        assert_eq!(fresh.person_direction.as_deref(), Some(direction.as_str()));
        assert_eq!(fresh.answers.len(), 2);
    }

    #[tokio::test]
    async fn a_cancelled_run_keeps_them_on_the_ticket_for_the_next_run() {
        let (store, s) = settled_at(Stage::Cancelled, &burst()).await;
        let fx = effects(&s, None);
        assert_eq!(fx.round, None);
        assert_eq!(fx.restart, None, "a run a person stopped never restarts");
        assert_eq!(fx.reply, None);
        assert_eq!(fx.ticket_notes.len(), 2);
        assert!(fx.run_notes[0].contains("kept on the ticket's record"));
        assert!(tc::open(store.all("TEAM#1", "ABC-1")).is_empty());
    }

    #[tokio::test]
    async fn a_stop_ends_the_run_and_is_said_on_the_ticket() {
        for stage in [
            Stage::Planning,
            Stage::Building,
            Stage::Reviewing,
            Stage::BetweenSteps,
        ] {
            let mut all = burst();
            all.push(comment("4", "Stop, we are dropping this.", Class::Stop));
            let (store, s) = settled_at(stage, &all).await;
            let fx = effects(&s, None);
            assert_eq!(fx.stop_by.as_deref(), Some("Pat Doe"), "at {stage:?}");
            assert!(fx.folded.is_empty() && fx.round.is_none() && fx.restart.is_none());
            assert!(fx.reply.unwrap().contains("I stopped the run"));
            // The other comments stay open for the next run on the ticket.
            assert_eq!(tc::open(store.all("TEAM#1", "ABC-1")).len(), 3);
        }
    }

    #[test]
    fn a_run_gone_quiet_is_not_working() {
        let now = chrono::Utc::now();
        let ago = |h: i64| (now - chrono::Duration::hours(h)).to_rfc3339();
        assert!(!gone_quiet("running", &ago(0), now));
        assert!(gone_quiet("running", &ago(3), now));
        assert!(gone_quiet("queued", &ago(2), now));
        assert!(!gone_quiet("awaiting_ci", &ago(20), now));
        assert!(gone_quiet("awaiting_ci", &ago(27), now));
        assert!(!gone_quiet("completed", &ago(100), now));
        assert!(!gone_quiet("running", "", now));
    }

    #[test]
    fn an_issues_existing_comments_are_context_for_the_run_that_starts() {
        let comments = vec![
            json!({"id": 1, "user": {"login": "pat-doe", "type": "User"},
                   "body": "Production only.", "created_at": "2026-01-01T00:00:00Z"}),
            json!({"id": 2, "user": {"login": "coderhelm[bot]", "type": "Bot"},
                   "body": "Questions only a person can answer"}),
            json!({"id": 3, "user": {"login": "ci-reporter[bot]", "type": "Bot"}, "body": "Coverage 81%"}),
            json!({"id": 4, "user": {"login": "sam-roe", "type": "User"}, "body": "  "}),
            json!({"id": 5, "user": {"login": "sam-roe", "type": "User"}, "body": "Stop for now."}),
        ];
        let kept = existing_github("GH-12", &comments);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].comment_id, "1");
        assert_eq!(kept[0].source, Source::Github);
        // Written before the run started: it can never stop the run.
        assert!(kept.iter().all(|c| !c.during_run()));
        assert_eq!(
            tc::decide(Class::Stop, Stage::Planning, kept[1].during_run(), false),
            Action::Fold
        );
        let many: Vec<serde_json::Value> = (0..60)
            .map(|i| json!({"id": i, "user": {"login": "pat-doe", "type": "User"}, "body": "x"}))
            .collect();
        let kept = existing_github("GH-12", &many);
        assert_eq!(kept.len(), MAX_EXISTING);
        assert_eq!(kept.last().unwrap().comment_id, "59");
    }

    #[test]
    fn the_pipeline_reads_comments_at_its_boundaries_and_when_a_step_ends() {
        let src = include_str!("mod.rs");
        let start = src.find("\nasync fn run_passes(").unwrap();
        let end = start + src[start..].find("\nasync fn hand_off_at(").unwrap();
        let run = &src[start..end];
        assert!(
            run.matches("read_ticket_comments(state, msg, run_id, usage)")
                .count()
                >= 5,
            "the run start and the plan, implement, test and pull request boundaries"
        );
        assert!(run.contains("ticket_comments::keep_existing_github("));
        // A run started by an older Jira app says earlier comments were
        // not read.
        assert!(run.contains("ticket_comments::note_unread_earlier(state, msg, run_id)"));
        let orch = &src[src.find("\npub async fn orchestrate_ticket(").unwrap()..start];
        assert!(orch.contains("ticket_comments::after_step("));
        // The run's questions are kept for reading the answers.
        let report = &src[src.find("\nasync fn report_questions(").unwrap()..];
        assert!(report.contains("ticket_comments::keep_asked("));
        // A round on the pull request reads them when it ends.
        let main = include_str!("../main.rs");
        assert_eq!(main.matches("ticket_comments::after_round(").count(), 2);
        // The comment reader is told at once, before anything else.
        let jc = include_str!("jira_comment.rs");
        let run_fn = &jc[jc.find("\npub async fn run(").unwrap()..];
        assert!(
            run_fn.find("ticket_comments::on_comment(").unwrap()
                < run_fn.find("classify(state").unwrap()
        );
    }
}
