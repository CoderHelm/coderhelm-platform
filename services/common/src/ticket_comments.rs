//! What a person writes on a ticket while CoderHelm works on it is kept,
//! acknowledged and acted on.
//!
//! A run once asked three questions on its ticket, proposed defaults and
//! kept building. A person answered on the ticket twenty minutes later. The
//! gateway logged that a run was in progress and dropped the comment:
//! nothing acknowledged it, the run never saw it, and its notes kept saying
//! it went on without the answers.
//!
//! Every ticket source now goes through one path:
//!
//! 1. The gateway keeps the comment ([`Store::record`]), once per comment
//!    id, so a webhook delivered twice is one comment. A comment that
//!    arrives while a run works is acknowledged on the ticket ([`ack_for`]:
//!    a reaction where the source has them, otherwise one short reply), and
//!    never when CoderHelm or another bot wrote it ([`is_person`]).
//! 2. The run reads its open comments at its next step boundary, all of
//!    them together, and the model says what each is ([`Class`]).
//!    [`decide`] says what happens for the run's [`Stage`]: an answer or an
//!    instruction is folded into the plan while planning, becomes a round
//!    on the pull request once the code is being built, and a comment that
//!    says to stop ends the run at once.
//! 3. A comment is settled in one conditional write ([`Store::settle`])
//!    before anything is done with it, so two readers never act on it
//!    twice, and one that nobody read stays open for whatever comes next:
//!    the end of the step, the next step, a round on the finished run's
//!    pull request, or the next run on the ticket.
//! 4. Comments written before the run started are kept the same way and
//!    read at its first boundary. They are context: one of them never
//!    stops a run that started after it.

use crate::run_questions::Answer;
use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// The settings-table sort key prefix of a ticket's comments.
pub const SK_PREFIX: &str = "TICKETCOMMENT#";
/// Days a comment written while a run worked is kept. Nothing reads a
/// comment after it is acted on; the record is kept only so that the same
/// comment delivered again is not taken twice, and so that one nobody read
/// (its worker stopped, or its round could not be sent) is still there for
/// what comes next. A run and its wait for CI end within 26 hours, and the
/// rounds on its open pull request follow within days: two weeks covers
/// them with room to spare.
pub const KEEP_DAYS: i64 = 14;
/// Days a comment written before a run is kept. It is stored only for a
/// ticket CoderHelm is already set to work (see the gateway), so the run
/// that reads it starts within minutes; a week covers a run held back by a
/// usage limit or a ticket lock.
pub const KEEP_DAYS_NO_RUN: i64 = 7;

/// How long a comment's record is kept, in days.
pub fn keep_days(c: &Comment) -> i64 {
    if c.during_run() {
        KEEP_DAYS
    } else {
        KEEP_DAYS_NO_RUN
    }
}
/// A comment's text as kept, in characters.
pub const MAX_TEXT_CHARS: usize = 4_000;
/// Comments read together at most; the oldest open ones first.
pub const MAX_BURST: usize = 20;
/// Run-record field: the questions the run told the ticket about and kept
/// building without (JSON list of strings).
pub const FIELD_ASKED: &str = "ticket_questions";
/// Run-record field read (never written) here: the questions a run that
/// stopped to ask kept as asked (`run_questions::Asked`).
pub const FIELD_STOPPED_TO_ASK: &str = crate::run_questions::FIELD_QUESTIONS;
/// The question an instruction (not an answer to a question) is kept under
/// when it is folded into the plan.
pub const COMMENT_QUESTION: &str = "A person's comment on the ticket";
/// Ticket-record field (`ticket_state::sk`): whether the event that last
/// started a run on the ticket carried the ticket's existing comments
/// ([`EVENT_COMMENTS_SENT`], [`EVENT_COMMENTS_NOT_SENT`] or
/// [`EVENT_COMMENTS_NOT_SENT_WEBHOOK`]). An event without them leaves a
/// run without what people wrote before it started, and the run says so.
pub const FIELD_EVENT_COMMENTS: &str = "event_comments";
pub const EVENT_COMMENTS_SENT: &str = "sent";
/// The Jira app sent the event without the ticket's comments (a version
/// older than the one that sends them).
pub const EVENT_COMMENTS_NOT_SENT: &str = "not_sent";
/// The event came from a plain Jira webhook, which never carries them.
pub const EVENT_COMMENTS_NOT_SENT_WEBHOOK: &str = "not_sent_webhook";

/// Whether a pre-run comment is stored at all. Only for a ticket
/// CoderHelm has something to do with: one it already ran on, or one
/// that carries the team's trigger label or is assigned to CoderHelm
/// when the comment is written. Comments on every other ticket are never
/// stored: when a run starts there, the event that starts it brings the
/// ticket's comments (Jira), or the run reads them from the issue
/// (GitHub).
pub fn stores_before_run(has_run: bool, has_trigger_label: bool, assigned: bool) -> bool {
    has_run || has_trigger_label || assigned
}

/// What a run on a Jira ticket says when the event that started it
/// carried no comments: it did not read what people wrote before it
/// started, and why, which differs by how the event arrived. `None` when
/// the comments were sent, or nothing is known.
///
/// CoderHelm cannot read them another way on either path: the Jira app's
/// web triggers return no comments, and a plain Jira webhook gives
/// CoderHelm an address and a signing secret, never access to the site.
pub fn unread_earlier_note(marker: Option<&str>) -> Option<&'static str> {
    match marker {
        Some(EVENT_COMMENTS_NOT_SENT) => Some(
            "Comments written on the ticket before this run started were not read: the \
             event that started the run did not include them (this site's Jira app is a \
             version that does not send them), and CoderHelm has no other access to them. \
             The run works from the ticket's title and description; comments written from \
             now on are read. Updating the Jira app on the site sends earlier comments with \
             the next ticket.",
        ),
        Some(EVENT_COMMENTS_NOT_SENT_WEBHOOK) => Some(
            "Comments written on the ticket before this run started were not read: the \
             event that started the run did not include the ticket's comments (it came \
             from a Jira webhook, which sends none with a ticket), and a webhook gives \
             CoderHelm no access to the Jira site to read them. The run works from the \
             ticket's title and description; comments written from now on are read.",
        ),
        _ => None,
    }
}

/// How CoderHelm's acknowledgement on a ticket starts. A comment that
/// starts this way is its own, whoever the tracker says wrote it.
pub const ACK_OPENING: &str = "Got it.";

/// Where the ticket lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    #[default]
    Jira,
    Github,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Jira => "jira",
            Source::Github => "github",
        }
    }
}

/// Where a comment stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Kept, nothing done with it yet.
    #[default]
    Pending,
    /// Read while the code was being built: it becomes a round on the pull
    /// request when the step ends.
    Deferred,
    /// Acted on (or left alone on purpose); `outcome` says which.
    Handled,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Pending => "pending",
            State::Deferred => "deferred",
            State::Handled => "handled",
        }
    }

    pub fn parse(s: &str) -> State {
        match s {
            "deferred" => State::Deferred,
            "handled" => State::Handled,
            _ => State::Pending,
        }
    }
}

/// What a comment is, as the model read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    /// Answers one of the run's open questions.
    Answer,
    /// A new instruction or a correction for this ticket.
    Instruction,
    /// Says to stop the work.
    Stop,
    /// Not for CoderHelm (people talking to each other, a status note).
    NotForCoderhelm,
}

impl Class {
    pub fn parse(s: &str) -> Option<Class> {
        match s
            .trim()
            .to_ascii_lowercase()
            .replace(['-', ' '], "_")
            .as_str()
        {
            "answer" => Some(Class::Answer),
            "instruction" | "correction" | "new_instruction" => Some(Class::Instruction),
            "stop" => Some(Class::Stop),
            "not_for_coderhelm" | "not_for_us" | "other" => Some(Class::NotForCoderhelm),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Class::Answer => "answer",
            Class::Instruction => "instruction",
            Class::Stop => "stop",
            Class::NotForCoderhelm => "not_for_coderhelm",
        }
    }
}

/// One comment a person wrote on a ticket.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Comment {
    pub ticket_id: String,
    pub source: Source,
    /// The comment's id in its tracker.
    pub comment_id: String,
    pub author: String,
    /// The tracker's id of the author (Jira's account id): a reply to the
    /// comment mentions them by it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub author_id: String,
    pub text: String,
    /// When CoderHelm received it (RFC 3339).
    pub at: String,
    /// The run that was working when it arrived; empty when none was (a
    /// comment written before the run started).
    #[serde(default)]
    pub run_id: String,
    #[serde(default)]
    pub state: State,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<Class>,
    /// The open question it answers, as asked; empty otherwise.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub question: String,
    /// What CoderHelm did with it, in one line.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub outcome: String,
    /// The run that settled it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub handled_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub handled_at: String,
}

impl Comment {
    /// Whether a run was working when the comment arrived. Only such a
    /// comment can stop a run.
    pub fn during_run(&self) -> bool {
        !self.run_id.is_empty()
    }

    pub fn is_open(&self) -> bool {
        self.state != State::Handled
    }
}

/// The sort key of every comment on `ticket_id`.
pub fn prefix(ticket_id: &str) -> String {
    format!("{SK_PREFIX}{}#", ticket_id.to_ascii_uppercase())
}

/// The sort key of one comment: the same comment always has the same key.
pub fn sk(ticket_id: &str, source: Source, comment_id: &str) -> String {
    format!("{}{}-{}", prefix(ticket_id), source.as_str(), comment_id)
}

/// The id a comment is kept under: the tracker's own, else one made from
/// who wrote it and what it says, so the same delivery is still one record.
pub fn comment_id(given: &str, author: &str, text: &str) -> String {
    let given = given.trim();
    if !given.is_empty() {
        return given.to_string();
    }
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(author.trim().as_bytes());
    h.update([0]);
    h.update(text.trim().as_bytes());
    format!("h{}", &hex::encode(h.finalize())[..16])
}

/// Whether a person wrote the comment. CoderHelm's own comments and other
/// bots' never count: `kind` is the tracker's account type (`Bot` on
/// GitHub, `app` in Jira), and a Jira comment under CoderHelm's name or
/// starting with its acknowledgement is its own.
pub fn is_person(source: Source, author: &str, kind: &str, text: &str) -> bool {
    let name = author.trim().to_ascii_lowercase();
    let kind = kind.trim().to_ascii_lowercase();
    if matches!(kind.as_str(), "bot" | "app") || name.ends_with("[bot]") {
        return false;
    }
    if text.trim_start().starts_with(ACK_OPENING) {
        return false;
    }
    match source {
        // Jira shows the app by its display name.
        Source::Jira => !name.contains("coderhelm"),
        // On GitHub a person whose login contains the word is still a
        // person; the app's own account is a Bot (above).
        Source::Github => true,
    }
}

/// How a comment is acknowledged on the ticket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ack {
    /// A reaction on the comment (GitHub).
    Reaction,
    /// One short reply (Jira has no reactions an app can add).
    Reply,
    None,
}

/// The acknowledgement for a comment: only for a person's, only while a run
/// works on the ticket, and only the first time the comment is seen.
pub fn ack_for(source: Source, person: bool, newly_recorded: bool, run_working: bool) -> Ack {
    if !person || !newly_recorded || !run_working {
        return Ack::None;
    }
    match source {
        Source::Github => Ack::Reaction,
        Source::Jira => Ack::Reply,
    }
}

/// The one short reply on a ticket without reactions.
pub fn ack_reply() -> String {
    format!(
        "{ACK_OPENING} I'm working on this ticket now. I read this comment at my next step and \
         act on it."
    )
}

/// Whether a run in this status is working on the ticket.
pub fn run_working(status: &str) -> bool {
    matches!(status, "running" | "queued" | "awaiting_ci")
}

/// Where the run is when its comments are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// Triage or planning: the plan isn't final.
    Planning,
    /// The plan is final and the code is being written or checked.
    Building,
    /// The work is built: the pull request is being opened, reviewed or
    /// its checks watched by the run's own step.
    Reviewing,
    /// One step ended and the next hasn't started.
    BetweenSteps,
    /// The run ended. `pr_open`: its pull request is open. `waiting`: it
    /// ended waiting on a person or failed, with no pull request.
    Finished { pr_open: bool, waiting: bool },
    /// A person stopped the run.
    Cancelled,
}

/// The stage of a run from its record. `in_step`: read by the run's own
/// step at a boundary (the run is working whatever the record says about
/// a status another writer set a moment ago).
pub fn stage_of(
    status: &str,
    current_pass: &str,
    pr_open: bool,
    in_step: bool,
    stale: bool,
) -> Stage {
    if crate::run_status::ended_by_person(status) {
        return Stage::Cancelled;
    }
    match status {
        "running" | "queued" if !stale => {
            if !in_step {
                return Stage::BetweenSteps;
            }
            match current_pass.trim().to_ascii_lowercase().as_str() {
                "" | "triage" | "plan" => Stage::Planning,
                "pr" | "review" | "checks" | "ready" | "await_merge" => Stage::Reviewing,
                _ => Stage::Building,
            }
        }
        "awaiting_ci" | "completed" | "success" if pr_open => Stage::Finished {
            pr_open: true,
            waiting: false,
        },
        // A run left `running` long after its worker stopped is a failed
        // run nobody marked yet.
        "running" | "queued" | "needs_input" | "needs_outside_change" | "failed" => {
            Stage::Finished {
                pr_open: false,
                waiting: true,
            }
        }
        _ => Stage::Finished {
            pr_open: false,
            waiting: false,
        },
    }
}

/// What is done with a comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Action {
    /// End the run now.
    Stop,
    /// Into the plan: the planner reads it word for word.
    Fold,
    /// Kept for the round on the pull request when this step ends.
    Defer,
    /// A feedback round on the open pull request, now.
    Round,
    /// Start the ticket again with it (the run ended waiting or failed).
    Restart,
    /// Kept on the ticket's record: the next run on the ticket reads it.
    Keep,
    /// Not for CoderHelm: left as it is, no reply.
    Leave,
    /// Nothing yet: the next step reads it.
    Wait,
}

/// What happens with a comment of `class` at `stage`. `during_run`: it
/// arrived while a run worked (only then can it stop one). `deferred`: an
/// earlier boundary already kept it for the pull request.
pub fn decide(class: Class, stage: Stage, during_run: bool, deferred: bool) -> Action {
    let working = matches!(
        stage,
        Stage::Planning | Stage::Building | Stage::Reviewing | Stage::BetweenSteps
    );
    match class {
        Class::NotForCoderhelm => return Action::Leave,
        Class::Stop if during_run && working => return Action::Stop,
        _ => {}
    }
    match stage {
        Stage::BetweenSteps => Action::Wait,
        Stage::Planning if deferred => Action::Wait,
        Stage::Planning => Action::Fold,
        Stage::Building | Stage::Reviewing if deferred => Action::Wait,
        Stage::Building | Stage::Reviewing => Action::Defer,
        Stage::Finished { pr_open: true, .. } => Action::Round,
        // A stop written to a run that already ended has nothing to stop.
        Stage::Finished { waiting: true, .. } if class != Class::Stop => Action::Restart,
        Stage::Finished { .. } | Stage::Cancelled => Action::Keep,
    }
}

// ── Reading: one model call for every unread comment ──────────────────

pub const SYSTEM: &str = "You read the comments people wrote on a ticket that an automated \
     coding agent (CoderHelm) is working on, and say what each one is. \
     answer: it answers one of the agent's open questions (give that question's number), in any \
     wording, including a short choice such as \"the second option\" or \"yes, production only\". \
     instruction: it asks for new or different work on this ticket, corrects the plan, or gives \
     the agent a fact it needs for the work. \
     stop: it tells the agent to stop working on this ticket (\"stop\", \"hold off\", \"don't \
     build this\", \"cancel\"). A request to stop doing one thing inside the work (\"stop adding \
     tests\") is an instruction, not stop. \
     not_for_coderhelm: people talking to each other, a status note (\"moved to QA\"), thanks, \
     or anything else that asks nothing of the agent and tells it nothing about the work. \
     Judge each comment as a whole. When in doubt between instruction and not_for_coderhelm, \
     choose instruction only if the agent's work on this ticket would change.";

pub fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "comments": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "n": {"type": "integer"},
                        "class": {"type": "string", "enum": ["answer", "instruction", "stop", "not_for_coderhelm"]},
                        "question": {"type": "integer"},
                        "reason": {"type": "string"}
                    },
                    "required": ["n", "class", "question", "reason"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["comments"],
        "additionalProperties": false
    })
}

/// The prompt: the ticket, the run's open questions (numbered from 1) and
/// the comments (numbered from 1).
pub fn prompt(title: &str, questions: &[String], comments: &[Comment]) -> String {
    let mut out = format!(
        "## Ticket\n{}\n\n## The agent's open questions\n",
        title.trim()
    );
    if questions.is_empty() {
        out.push_str("(none)\n");
    }
    for (i, q) in questions.iter().enumerate() {
        out.push_str(&format!("{}. {}\n", i + 1, one_line(q, 600)));
    }
    out.push_str("\n## The comments\n");
    for (i, c) in comments.iter().enumerate() {
        out.push_str(&format!(
            "### Comment {} (by {})\n{}\n\n",
            i + 1,
            if c.author.trim().is_empty() {
                "a person"
            } else {
                c.author.trim()
            },
            crate::truncate_str(c.text.trim(), MAX_TEXT_CHARS)
        ));
    }
    out.push_str(
        "For each comment give its number (n), its class, and the number of the open question \
         it answers (0 when it answers none).\n",
    );
    out
}

/// One comment as read: its class and the open question it answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reading {
    pub class: Class,
    pub question: String,
}

/// The reading of each comment from the model's reply, in the comments'
/// order. A comment the reply leaves out or names no class for reads as
/// [`unread`]; an answer that names no open question is an instruction.
pub fn readings(reply: &Value, questions: &[String], comments: usize) -> Vec<Reading> {
    let mut out: Vec<Reading> = (0..comments).map(|_| unread()).collect();
    for item in reply["comments"].as_array().into_iter().flatten() {
        let n = item["n"].as_u64().unwrap_or(0) as usize;
        if n == 0 || n > comments {
            continue;
        }
        let Some(class) = Class::parse(item["class"].as_str().unwrap_or("")) else {
            continue;
        };
        let q = item["question"].as_u64().unwrap_or(0) as usize;
        let question = match q {
            0 => String::new(),
            n => questions.get(n - 1).cloned().unwrap_or_default(),
        };
        out[n - 1] = match class {
            Class::Answer if question.is_empty() => Reading {
                class: Class::Instruction,
                question,
            },
            Class::Answer => Reading { class, question },
            _ => Reading {
                class,
                question: String::new(),
            },
        };
    }
    out
}

/// A comment the model couldn't read: an instruction, so the planner or
/// the round on the pull request reads it again. Never a stop.
pub fn unread() -> Reading {
    Reading {
        class: Class::Instruction,
        question: String::new(),
    }
}

// ── What a comment becomes ────────────────────────────────────────────

/// The comment as the planner reads it: an answer to the question it
/// answers, or a person's comment on the ticket.
pub fn as_answer(c: &Comment) -> Answer {
    Answer {
        question_id: format!("ticket-comment-{}", c.comment_id),
        question: if c.question.trim().is_empty() {
            COMMENT_QUESTION.to_string()
        } else {
            c.question.clone()
        },
        answer: crate::truncate_str(c.text.trim(), crate::run_questions::MAX_ANSWER_CHARS)
            .to_string(),
        author: c.author.clone(),
        answered_at: c.at.clone(),
        ..Default::default()
    }
}

/// What the round on the pull request is asked to do with the comments.
pub fn round_body(comments: &[Comment]) -> String {
    let mut out = String::from(
        "People wrote on the ticket while this run was working, after its plan was final. \
         Apply each of these on this pull request.\n",
    );
    for c in comments {
        let who = who(c);
        let text = crate::truncate_str(c.text.trim(), MAX_TEXT_CHARS);
        if c.class == Some(Class::Answer) && !c.question.trim().is_empty() {
            out.push_str(&format!(
                "\n### Answer from {who} to the run's question\n\
                 Question: {}\nAnswer: {text}\n\
                 The run asked this on the ticket, built its own default and went on. Where \
                 this answer differs from the default that was built, change the code in the \
                 place the plan put that choice (and its tests). Where it matches, change \
                 nothing.\n",
                one_line(&c.question, 600)
            ));
        } else {
            out.push_str(&format!(
                "\n### Comment from {who}\n{text}\n\
                 Read it as an instruction or correction for this ticket's work and apply it \
                 to the code on this pull request.\n"
            ));
        }
    }
    out.push_str(
        "\nSay in the pull request, for each one, what changed because of it, or that the code \
         already matched it.\n",
    );
    crate::truncate_str(&out, crate::run_round::MAX_FEEDBACK_CHARS).to_string()
}

/// The direction a restarted run carries ("author: words"), from the
/// comments that start it.
pub fn direction(comments: &[Comment]) -> String {
    comments
        .iter()
        .map(|c| format!("{}: {}", who(c), one_line(&c.text, 600)))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The one line kept on the comment and shown on the run for what was done.
pub fn outcome(action: Action, c: &Comment, pr: Option<&str>) -> String {
    let what = match (c.class, c.question.trim().is_empty()) {
        (Some(Class::Answer), false) => {
            format!("an answer to \"{}\"", one_line(&c.question, 160))
        }
        (Some(Class::Stop), _) => "a request to stop".to_string(),
        (Some(Class::NotForCoderhelm), _) => "not for CoderHelm".to_string(),
        _ => "an instruction for this ticket".to_string(),
    };
    let pr = pr.filter(|p| !p.is_empty()).unwrap_or("the pull request");
    let did = match action {
        Action::Stop => "stopped the run".to_string(),
        Action::Fold => "folded into the plan before any code was written".to_string(),
        Action::Defer => {
            "the plan is final, so it is applied as a round on the pull request when this step \
             ends"
                .to_string()
        }
        Action::Round => format!("applying it as a round on {pr}"),
        Action::Restart => "started the ticket again with it".to_string(),
        Action::Keep => {
            "kept on the ticket's record; the next run on this ticket reads it".to_string()
        }
        Action::Leave => "left as it is".to_string(),
        Action::Wait => "the next step reads it".to_string(),
    };
    format!("Read {}'s comment as {what}: {did}.", who(c))
}

/// The note the run page shows when a comment arrives during a run.
pub fn received_note(c: &Comment, ack: Ack) -> String {
    let acked = match ack {
        Ack::Reaction => " Acknowledged there with a reaction.",
        Ack::Reply => " Acknowledged there with a reply.",
        Ack::None => "",
    };
    format!(
        "{} commented on the ticket: \"{}\".{acked} The run reads it at its next step.",
        who(c),
        one_line(&c.text, 300)
    )
}

/// What CoderHelm says on the ticket about the comments it acted on (one
/// comment for all of them); `None` when there is nothing to say (every
/// one was left alone, folded quietly into the plan, or is still waiting).
pub fn ticket_reply(done: &[(Action, Comment)], pr: Option<&str>) -> Option<String> {
    let said: Vec<String> = done
        .iter()
        .filter(|(a, c)| {
            matches!(a, Action::Stop | Action::Round | Action::Restart)
                || (*a == Action::Fold && !c.question.trim().is_empty())
        })
        .map(|(a, c)| {
            let pr = pr.filter(|p| !p.is_empty()).unwrap_or("the pull request");
            match (a, c.class, c.question.trim().is_empty()) {
                (Action::Stop, _, _) => format!(
                    "{} asked me to stop, so I stopped the run. What is built so far stays on \
                     its branch. Assign the ticket again, or reply here, and I take it up from \
                     there.",
                    who(c)
                ),
                (Action::Round, Some(Class::Answer), false) => format!(
                    "{}'s answer to \"{}\": I built my default for this before the answer came. \
                     I'm now changing the code where that choice lives to match the answer, on \
                     {pr}, and I say there what changed.",
                    who(c),
                    one_line(&c.question, 200)
                ),
                (Action::Fold, _, _) => format!(
                    "{}'s answer to \"{}\": using it (\"{}\") in the plan I'm making now.",
                    who(c),
                    one_line(&c.question, 200),
                    one_line(&c.text, 160)
                ),
                (Action::Round, _, _) => format!(
                    "{}'s comment (\"{}\"): I'm applying it on {pr} now, and I say there what \
                     changed.",
                    who(c),
                    one_line(&c.text, 160)
                ),
                _ => format!(
                    "{}'s comment (\"{}\"): I'm starting the work on this ticket again with it.",
                    who(c),
                    one_line(&c.text, 160)
                ),
            }
        })
        .collect();
    (!said.is_empty()).then(|| said.join("\n\n"))
}

fn who(c: &Comment) -> &str {
    if c.author.trim().is_empty() {
        "A person"
    } else {
        c.author.trim()
    }
}

fn one_line(s: &str, max: usize) -> String {
    let flat = s.split_whitespace().collect::<Vec<_>>().join(" ");
    crate::truncate_str(&flat, max).to_string()
}

/// The open comments of a ticket, oldest first, at most [`MAX_BURST`].
pub fn open(mut all: Vec<Comment>) -> Vec<Comment> {
    all.retain(Comment::is_open);
    all.sort_by(|a, b| a.at.cmp(&b.at).then(a.comment_id.cmp(&b.comment_id)));
    all.truncate(MAX_BURST);
    all
}

// ── Where comments are kept ───────────────────────────────────────────

/// Where ticket comments are kept. DynamoDB in production ([`Dynamo`]); a
/// map in tests ([`Memory`]).
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Keeps a new comment. `Ok(false)` when it is already kept (the same
    /// comment delivered again): nothing is written.
    async fn record(&self, team_id: &str, c: &Comment) -> Result<bool, String>;
    /// Every comment kept for the ticket.
    async fn of_ticket(&self, team_id: &str, ticket_id: &str) -> Result<Vec<Comment>, String>;
    /// Writes `c` only while the kept comment is still in state `from`.
    /// `Ok(false)` when another reader settled it first.
    async fn settle(&self, team_id: &str, c: &Comment, from: State) -> Result<bool, String>;
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn brief(e: impl std::fmt::Debug) -> String {
    format!("{e:?}").chars().take(300).collect()
}

/// Ticket comments on the settings table (`pk` = team, `sk` = [`sk`]).
pub struct Dynamo<'a> {
    pub client: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
}

#[async_trait::async_trait]
impl Store for Dynamo<'_> {
    async fn record(&self, team_id: &str, c: &Comment) -> Result<bool, String> {
        let doc = serde_json::to_string(c).map_err(brief)?;
        let now = chrono::Utc::now();
        let res = self
            .client
            .put_item()
            .table_name(self.table)
            .item("pk", s(team_id))
            .item("sk", s(&sk(&c.ticket_id, c.source, &c.comment_id)))
            .item("comment_state", s(c.state.as_str()))
            .item("doc", s(&doc))
            .item("updated_at", s(&now.to_rfc3339()))
            // The settings table expires items on `ttl`.
            .item(
                "ttl",
                AttributeValue::N(
                    (now + chrono::Duration::days(keep_days(c)))
                        .timestamp()
                        .to_string(),
                ),
            )
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;
        match res {
            Ok(_) => Ok(true),
            Err(e) if brief(&e).contains("ConditionalCheckFailed") => Ok(false),
            Err(e) => Err(brief(e)),
        }
    }

    async fn of_ticket(&self, team_id: &str, ticket_id: &str) -> Result<Vec<Comment>, String> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let page = self
                .client
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                .expression_attribute_values(":pk", s(team_id))
                .expression_attribute_values(":p", s(&prefix(ticket_id)))
                .consistent_read(true)
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(brief)?;
            for item in page.items() {
                let Some(mut c) = item
                    .get("doc")
                    .and_then(|v| v.as_s().ok())
                    .and_then(|d| serde_json::from_str::<Comment>(d).ok())
                else {
                    continue;
                };
                // The state attribute is what conditional writes check.
                if let Some(st) = item.get("comment_state").and_then(|v| v.as_s().ok()) {
                    c.state = State::parse(st);
                }
                out.push(c);
            }
            start = page.last_evaluated_key().cloned();
            if start.is_none() {
                break;
            }
        }
        Ok(out)
    }

    async fn settle(&self, team_id: &str, c: &Comment, from: State) -> Result<bool, String> {
        let doc = serde_json::to_string(c).map_err(brief)?;
        let res = self
            .client
            .update_item()
            .table_name(self.table)
            .key("pk", s(team_id))
            .key("sk", s(&sk(&c.ticket_id, c.source, &c.comment_id)))
            .update_expression("SET comment_state = :to, doc = :doc, updated_at = :t")
            .condition_expression("comment_state = :from")
            .expression_attribute_values(":to", s(c.state.as_str()))
            .expression_attribute_values(":from", s(from.as_str()))
            .expression_attribute_values(":doc", s(&doc))
            .expression_attribute_values(":t", s(&chrono::Utc::now().to_rfc3339()))
            .send()
            .await;
        match res {
            Ok(_) => Ok(true),
            Err(e) if brief(&e).contains("ConditionalCheckFailed") => Ok(false),
            Err(e) => Err(brief(e)),
        }
    }
}

/// Comments in memory, with the same semantics as [`Dynamo`]: a comment is
/// kept once, and a settle is conditional on the state it was read in.
#[derive(Default)]
pub struct Memory {
    pub items: std::sync::Mutex<BTreeMap<(String, String), Comment>>,
}

impl Memory {
    pub fn all(&self, team_id: &str, ticket_id: &str) -> Vec<Comment> {
        let p = prefix(ticket_id);
        self.items
            .lock()
            .unwrap()
            .iter()
            .filter(|((pk, sk), _)| pk == team_id && sk.starts_with(&p))
            .map(|(_, c)| c.clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl Store for Memory {
    async fn record(&self, team_id: &str, c: &Comment) -> Result<bool, String> {
        let key = (
            team_id.to_string(),
            sk(&c.ticket_id, c.source, &c.comment_id),
        );
        let mut items = self.items.lock().unwrap();
        if items.contains_key(&key) {
            return Ok(false);
        }
        items.insert(key, c.clone());
        Ok(true)
    }

    async fn of_ticket(&self, team_id: &str, ticket_id: &str) -> Result<Vec<Comment>, String> {
        Ok(self.all(team_id, ticket_id))
    }

    async fn settle(&self, team_id: &str, c: &Comment, from: State) -> Result<bool, String> {
        let key = (
            team_id.to_string(),
            sk(&c.ticket_id, c.source, &c.comment_id),
        );
        let mut items = self.items.lock().unwrap();
        match items.get_mut(&key) {
            Some(kept) if kept.state == from => {
                *kept = c.clone();
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

// ── Reading a ticket's open comments, as one unit ─────────────────────

/// What a read of a ticket's open comments settled, by what is done with
/// each. The caller does the work (sends the round, stops the run); every
/// comment here was claimed in the store first, so no other reader acts on
/// it too.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Settled {
    pub done: Vec<(Action, Comment)>,
}

impl Settled {
    pub fn with(&self, action: Action) -> Vec<Comment> {
        self.done
            .iter()
            .filter(|(a, _)| *a == action)
            .map(|(_, c)| c.clone())
            .collect()
    }

    pub fn stopped(&self) -> bool {
        self.done.iter().any(|(a, _)| *a == Action::Stop)
    }
}

/// Gives each open, unread comment its reading (kept in the store, so a
/// later reader doesn't ask the model again). `read` is the model's reading
/// of the unread ones, in order; missing ones read as [`unread`].
pub async fn keep_readings(
    store: &dyn Store,
    team_id: &str,
    open: &mut [Comment],
    read: &[Reading],
) {
    let mut next = read.iter();
    for c in open.iter_mut().filter(|c| c.class.is_none()) {
        let r = next.next().cloned().unwrap_or_else(unread);
        c.class = Some(r.class);
        c.question = r.question;
        // Best effort: a reader that loses this write reads the comment
        // again at its own boundary.
        let _ = store.settle(team_id, c, c.state).await;
    }
}

/// Settles every open comment for `stage`: decides what happens with each
/// ([`decide`]) and claims it in the store. Comments that wait stay as they
/// are. When one says to stop, only it is settled: the rest stay open for
/// the next run on the ticket.
pub async fn settle_open(
    store: &dyn Store,
    team_id: &str,
    run_id: &str,
    open: &[Comment],
    stage: Stage,
    pr: Option<&str>,
) -> Settled {
    let now = chrono::Utc::now().to_rfc3339();
    let planned: Vec<(Action, &Comment)> = open
        .iter()
        .map(|c| {
            let class = c.class.unwrap_or(Class::Instruction);
            (
                decide(class, stage, c.during_run(), c.state == State::Deferred),
                c,
            )
        })
        .collect();
    let stop = planned.iter().any(|(a, _)| *a == Action::Stop);
    let mut out = Settled::default();
    for (action, c) in planned {
        if action == Action::Wait || (stop && action != Action::Stop) {
            continue;
        }
        let mut next = c.clone();
        next.outcome = outcome(action, c, pr);
        next.handled_by = run_id.to_string();
        next.handled_at = now.clone();
        next.state = if action == Action::Defer {
            State::Deferred
        } else {
            State::Handled
        };
        match store.settle(team_id, &next, c.state).await {
            Ok(true) => out.done.push((action, next)),
            // Another reader settled it first: it acts, this one doesn't.
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(ticket_id = %c.ticket_id, error = %e, "Could not settle a ticket comment; it stays open")
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comment(id: &str, text: &str) -> Comment {
        Comment {
            ticket_id: "ABC-1".into(),
            source: Source::Jira,
            comment_id: id.into(),
            author: "Pat Doe".into(),
            text: text.into(),
            at: format!("2026-01-01T00:00:{:0>2}Z", id),
            run_id: "01RUN".into(),
            ..Default::default()
        }
    }

    const WORKING: [Stage; 4] = [
        Stage::Planning,
        Stage::Building,
        Stage::Reviewing,
        Stage::BetweenSteps,
    ];

    #[tokio::test]
    async fn a_comment_is_kept_once_whatever_the_webhook_delivers() {
        let store = Memory::default();
        let c = comment("1", "Production only.");
        assert!(store.record("TEAM#1", &c).await.unwrap());
        // The same delivery again, and again after the run read it.
        assert!(!store.record("TEAM#1", &c).await.unwrap());
        let mut read = c.clone();
        read.state = State::Handled;
        assert!(store.settle("TEAM#1", &read, State::Pending).await.unwrap());
        assert!(!store.record("TEAM#1", &c).await.unwrap());
        let kept = store.all("TEAM#1", "ABC-1");
        assert_eq!(kept.len(), 1);
        assert_eq!(
            kept[0].state,
            State::Handled,
            "a redelivery never reopens it"
        );
        // Another ticket, another team and another source are other comments.
        assert!(store.record("TEAM#2", &c).await.unwrap());
        let mut gh = c.clone();
        gh.source = Source::Github;
        assert!(store.record("TEAM#1", &gh).await.unwrap());
    }

    #[test]
    fn the_key_is_the_comment_id_and_a_missing_id_is_still_one_key() {
        assert_eq!(
            sk("abc-1", Source::Jira, "10042"),
            "TICKETCOMMENT#ABC-1#jira-10042"
        );
        assert!(sk("ABC-1", Source::Jira, "1").starts_with(&prefix("abc-1")));
        // ABC-1's prefix never matches ABC-10's comments.
        assert!(!sk("ABC-10", Source::Jira, "1").starts_with(&prefix("ABC-1")));
        assert_eq!(comment_id(" 77 ", "a", "b"), "77");
        let made = comment_id("", "Pat Doe", "Production only.");
        assert_eq!(made, comment_id("", "Pat Doe", " Production only. "));
        assert_ne!(made, comment_id("", "Pat Doe", "Staging too."));
    }

    #[test]
    fn only_a_persons_comment_counts() {
        assert!(is_person(
            Source::Jira,
            "Pat Doe",
            "atlassian",
            "Production only."
        ));
        // CoderHelm's own comments, by name, by account type and by text.
        assert!(!is_person(Source::Jira, "CoderHelm", "app", "Questions"));
        assert!(!is_person(Source::Jira, "Coderhelm for Jira", "", "x"));
        assert!(!is_person(Source::Jira, "Automation", "app", "x"));
        assert!(!is_person(Source::Jira, "Renamed App", "", &ack_reply()));
        assert!(!is_person(Source::Github, "coderhelm[bot]", "Bot", "x"));
        assert!(!is_person(Source::Github, "ci-reporter[bot]", "", "x"));
        assert!(!is_person(Source::Github, "some-app", "Bot", "x"));
        // A person whose GitHub login contains the word is a person.
        assert!(is_person(Source::Github, "coderhelm-fan", "User", "x"));
    }

    #[test]
    fn one_acknowledgement_per_comment_and_none_for_bots() {
        assert_eq!(ack_for(Source::Github, true, true, true), Ack::Reaction);
        assert_eq!(ack_for(Source::Jira, true, true, true), Ack::Reply);
        for source in [Source::Github, Source::Jira] {
            // Delivered again: no second acknowledgement.
            assert_eq!(ack_for(source, true, false, true), Ack::None);
            // CoderHelm's own comment or another bot's.
            assert_eq!(ack_for(source, false, true, true), Ack::None);
            // No run is working: the comment is context for the next run.
            assert_eq!(ack_for(source, true, true, false), Ack::None);
        }
        // The reply is short, and is itself never read as a person's.
        assert!(ack_reply().len() < 120);
        assert!(!is_person(Source::Jira, "Someone", "", &ack_reply()));
    }

    #[test]
    fn the_stage_comes_from_the_run_record() {
        assert_eq!(
            stage_of("running", "triage", false, true, false),
            Stage::Planning
        );
        assert_eq!(
            stage_of("running", "plan", false, true, false),
            Stage::Planning
        );
        assert_eq!(stage_of("running", "", false, true, false), Stage::Planning);
        for pass in ["implement", "implement_started", "test", "security"] {
            assert_eq!(
                stage_of("running", pass, false, true, false),
                Stage::Building
            );
        }
        assert_eq!(
            stage_of("running", "pr", true, true, false),
            Stage::Reviewing
        );
        // Read outside a step: the run handed off to a fresh one.
        assert_eq!(
            stage_of("running", "implement", false, false, false),
            Stage::BetweenSteps
        );
        assert_eq!(
            stage_of("queued", "plan", false, false, false),
            Stage::BetweenSteps
        );
        let open = Stage::Finished {
            pr_open: true,
            waiting: false,
        };
        assert_eq!(stage_of("awaiting_ci", "checks", true, false, false), open);
        assert_eq!(stage_of("completed", "done", true, false, false), open);
        let waiting = Stage::Finished {
            pr_open: false,
            waiting: true,
        };
        assert_eq!(
            stage_of("needs_input", "plan", false, false, false),
            waiting
        );
        assert_eq!(
            stage_of("failed", "implement", false, false, false),
            waiting
        );
        // Left `running` long after its worker stopped.
        assert_eq!(
            stage_of("running", "implement", false, false, true),
            waiting
        );
        assert_eq!(
            stage_of("merged", "done", false, false, false),
            Stage::Finished {
                pr_open: false,
                waiting: false
            }
        );
        for status in ["cancelled", "closed"] {
            assert_eq!(
                stage_of(status, "implement", true, false, false),
                Stage::Cancelled
            );
        }
    }

    #[test]
    fn each_class_at_each_stage() {
        use Action::*;
        let pr_open = Stage::Finished {
            pr_open: true,
            waiting: false,
        };
        let waiting = Stage::Finished {
            pr_open: false,
            waiting: true,
        };
        let done = Stage::Finished {
            pr_open: false,
            waiting: false,
        };
        for class in [Class::Answer, Class::Instruction] {
            assert_eq!(decide(class, Stage::Planning, true, false), Fold);
            assert_eq!(decide(class, Stage::Building, true, false), Defer);
            assert_eq!(decide(class, Stage::Reviewing, true, false), Defer);
            assert_eq!(decide(class, Stage::BetweenSteps, true, false), Wait);
            assert_eq!(decide(class, pr_open, true, false), Round);
            assert_eq!(decide(class, waiting, true, false), Restart);
            assert_eq!(decide(class, done, true, false), Keep);
            assert_eq!(decide(class, Stage::Cancelled, true, false), Keep);
            // Kept for the pull request at an earlier boundary: it waits
            // through the rest of the step, then becomes the round.
            assert_eq!(decide(class, Stage::Building, true, true), Wait);
            assert_eq!(decide(class, Stage::Reviewing, true, true), Wait);
            assert_eq!(decide(class, Stage::BetweenSteps, true, true), Wait);
            assert_eq!(decide(class, pr_open, true, true), Round);
            assert_eq!(decide(class, waiting, true, true), Restart);
            assert_eq!(decide(class, Stage::Cancelled, true, true), Keep);
        }
        // Not for CoderHelm: left alone at every stage.
        for stage in WORKING
            .into_iter()
            .chain([pr_open, waiting, done, Stage::Cancelled])
        {
            assert_eq!(decide(Class::NotForCoderhelm, stage, true, false), Leave);
        }
    }

    #[test]
    fn a_stop_is_honoured_at_once_while_the_run_works() {
        for stage in WORKING {
            assert_eq!(decide(Class::Stop, stage, true, false), Action::Stop);
            // Even one an earlier boundary had kept for the pull request.
            assert_eq!(decide(Class::Stop, stage, true, true), Action::Stop);
        }
        // Written before the run started: context, never a stop.
        assert_eq!(
            decide(Class::Stop, Stage::Planning, false, false),
            Action::Fold
        );
        assert_eq!(
            decide(Class::Stop, Stage::Building, false, false),
            Action::Defer
        );
        // On a finished run with its pull request open, the round reads it.
        assert_eq!(
            decide(
                Class::Stop,
                Stage::Finished {
                    pr_open: true,
                    waiting: false
                },
                true,
                false
            ),
            Action::Round
        );
        // Nothing to stop, and never a restart.
        assert_eq!(
            decide(
                Class::Stop,
                Stage::Finished {
                    pr_open: false,
                    waiting: true
                },
                true,
                false
            ),
            Action::Keep
        );
        assert_eq!(
            decide(Class::Stop, Stage::Cancelled, true, false),
            Action::Keep
        );
    }

    #[test]
    fn the_reply_is_read_per_comment_and_a_gap_is_never_a_stop() {
        let questions = vec![
            "Which environments get the flag?".to_string(),
            "Keep the old column?".to_string(),
        ];
        let reply = json!({"comments": [
            {"n": 1, "class": "answer", "question": 2, "reason": ""},
            {"n": 2, "class": "stop", "question": 0, "reason": ""},
            {"n": 3, "class": "answer", "question": 0, "reason": ""},
            {"n": 4, "class": "not_for_coderhelm", "question": 1, "reason": ""},
            {"n": 9, "class": "stop", "question": 0, "reason": ""}
        ]});
        let r = readings(&reply, &questions, 5);
        assert_eq!(r[0].class, Class::Answer);
        assert_eq!(r[0].question, "Keep the old column?");
        assert_eq!(r[1].class, Class::Stop);
        // An answer that names no open question is an instruction.
        assert_eq!(r[2].class, Class::Instruction);
        assert_eq!(r[3].class, Class::NotForCoderhelm);
        assert!(r[3].question.is_empty());
        // Left out by the model: read again downstream, never a stop.
        assert_eq!(r[4], unread());
        assert_eq!(unread().class, Class::Instruction);
        // No reply at all.
        assert!(readings(&json!({}), &questions, 2)
            .iter()
            .all(|r| *r == unread()));
        let p = prompt("Add the flag", &questions, &[comment("1", "Yes, keep it.")]);
        assert!(p.contains("2. Keep the old column?"));
        assert!(p.contains("Comment 1 (by Pat Doe)"));
    }

    #[tokio::test]
    async fn a_burst_is_read_together_and_each_comment_is_acted_on_once() {
        let store = Memory::default();
        for (id, text) in [
            ("1", "Production only."),
            ("2", "Also rename the flag."),
            ("3", "Thanks all"),
        ] {
            store.record("TEAM#1", &comment(id, text)).await.unwrap();
        }
        let mut open_now = open(store.all("TEAM#1", "ABC-1"));
        assert_eq!(open_now.len(), 3);
        let read = vec![
            Reading {
                class: Class::Answer,
                question: "Which environments?".into(),
            },
            Reading {
                class: Class::Instruction,
                question: String::new(),
            },
            Reading {
                class: Class::NotForCoderhelm,
                question: String::new(),
            },
        ];
        keep_readings(&store, "TEAM#1", &mut open_now, &read).await;
        // The readings are kept: a second reader doesn't ask the model again.
        assert!(open(store.all("TEAM#1", "ABC-1"))
            .iter()
            .all(|c| c.class.is_some()));

        // Two readers at the same moment (the run's boundary and the read
        // right after the comment): each comment is settled by one of them.
        let a = settle_open(&store, "TEAM#1", "01RUN", &open_now, Stage::Building, None).await;
        let b = settle_open(&store, "TEAM#1", "01RUN", &open_now, Stage::Building, None).await;
        assert_eq!(a.done.len(), 3);
        assert!(b.done.is_empty(), "the second reader acts on nothing");
        assert_eq!(a.with(Action::Defer).len(), 2);
        assert_eq!(a.with(Action::Leave).len(), 1);

        // The step ends with the pull request open: the two kept for it
        // become one round, and the one left alone is not read again.
        let open_now = open(store.all("TEAM#1", "ABC-1"));
        assert_eq!(open_now.len(), 2);
        let end = settle_open(
            &store,
            "TEAM#1",
            "01RUN",
            &open_now,
            Stage::Finished {
                pr_open: true,
                waiting: false,
            },
            Some("acme/web#12"),
        )
        .await;
        let round = end.with(Action::Round);
        assert_eq!(round.len(), 2);
        let body = round_body(&round);
        assert!(body.contains("Question: Which environments?"));
        assert!(body.contains("Answer: Production only."));
        assert!(body.contains("change the code in the place the plan put that choice"));
        assert!(body.contains("Also rename the flag."));
        assert!(open(store.all("TEAM#1", "ABC-1")).is_empty());
        // Nothing left to act on, whoever reads next.
        let again = settle_open(
            &store,
            "TEAM#1",
            "01RUN",
            &open(store.all("TEAM#1", "ABC-1")),
            Stage::Finished {
                pr_open: true,
                waiting: false,
            },
            None,
        )
        .await;
        assert!(again.done.is_empty());
    }

    #[tokio::test]
    async fn a_stop_settles_only_itself_and_the_rest_stay_for_the_next_run() {
        let store = Memory::default();
        let mut stop = comment("1", "Stop, we are dropping this.");
        stop.class = Some(Class::Stop);
        let mut more = comment("2", "If we pick it up again, production only.");
        more.class = Some(Class::Instruction);
        store.record("TEAM#1", &stop).await.unwrap();
        store.record("TEAM#1", &more).await.unwrap();
        let open_now = open(store.all("TEAM#1", "ABC-1"));
        let s = settle_open(&store, "TEAM#1", "01RUN", &open_now, Stage::Building, None).await;
        assert!(s.stopped());
        assert_eq!(s.done.len(), 1);
        let left = open(store.all("TEAM#1", "ABC-1"));
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].comment_id, "2");
        assert_eq!(left[0].state, State::Pending);
        let said = ticket_reply(&s.done, None).unwrap();
        assert!(said.contains("I stopped the run"));
    }

    #[test]
    fn what_is_said_on_the_ticket_and_the_run() {
        let mut answer = comment("1", "Production only.");
        answer.class = Some(Class::Answer);
        answer.question = "Which environments get the flag?".into();
        let mut chat = comment("2", "Thanks");
        chat.class = Some(Class::NotForCoderhelm);
        let pr = Some("https://example.test/pr/12");
        // The answer applied on the pull request is said on the ticket.
        let said = ticket_reply(&[(Action::Round, answer.clone())], pr).unwrap();
        assert!(said.contains("Pat Doe's answer to \"Which environments get the flag?\""));
        assert!(said.contains("https://example.test/pr/12"));
        // Left alone, folded or waiting: no reply.
        assert_eq!(ticket_reply(&[(Action::Leave, chat.clone())], pr), None);
        assert!(ticket_reply(&[(Action::Fold, answer.clone())], pr)
            .unwrap()
            .contains("in the plan I'm making now"));
        assert_eq!(ticket_reply(&[(Action::Defer, answer.clone())], pr), None);
        // The run page says what arrived and what was done with it.
        assert!(received_note(&answer, Ack::Reply).contains(
            "Pat Doe commented on the ticket: \"Production only.\". Acknowledged there with a reply."
        ));
        assert!(outcome(Action::Fold, &answer, None)
            .starts_with("Read Pat Doe's comment as an answer to \"Which environments"));
        assert!(outcome(Action::Leave, &chat, None).contains("not for CoderHelm: left as it is"));
        // What the planner reads.
        let a = as_answer(&answer);
        assert_eq!(a.question, "Which environments get the flag?");
        assert_eq!(a.answer, "Production only.");
        let mut instruction = comment("3", "Use the existing helper.");
        instruction.class = Some(Class::Instruction);
        assert_eq!(as_answer(&instruction).question, COMMENT_QUESTION);
        assert_eq!(
            direction(&[answer, instruction]),
            "Pat Doe: Production only.\nPat Doe: Use the existing helper."
        );
    }

    #[test]
    fn a_comment_on_a_ticket_coderhelm_has_nothing_to_do_with_is_never_stored() {
        // No run on record, no trigger label, not assigned to CoderHelm.
        assert!(!stores_before_run(false, false, false));
        assert!(stores_before_run(true, false, false));
        assert!(stores_before_run(false, true, false));
        assert!(stores_before_run(false, false, true));
    }

    #[test]
    fn records_are_kept_two_weeks_and_a_week_before_a_run() {
        let during = comment("1", "x");
        let mut before = comment("2", "x");
        before.run_id.clear();
        assert_eq!(keep_days(&during), 14);
        assert_eq!(keep_days(&before), 7);
    }

    #[test]
    fn a_run_started_by_an_older_jira_app_says_earlier_comments_were_not_read() {
        let note = unread_earlier_note(Some(EVENT_COMMENTS_NOT_SENT)).unwrap();
        assert!(note.contains("before this run started were not read"));
        assert!(note.contains("this site's Jira app is a version that does not send them"));
        assert!(note.contains("Updating the Jira app"));
    }

    #[test]
    fn a_run_started_by_a_plain_jira_webhook_says_so_without_blaming_an_app() {
        let note = unread_earlier_note(Some(EVENT_COMMENTS_NOT_SENT_WEBHOOK)).unwrap();
        assert!(note.contains("before this run started were not read"));
        assert!(note.contains("did not include the ticket's comments"));
        assert!(note.contains("came from a Jira webhook"));
        // No app is involved on this path, so none is named or blamed.
        assert!(!note.to_lowercase().contains("app"));
        assert!(!note.contains("version"));
    }

    #[test]
    fn nothing_is_said_when_the_comments_came_or_nothing_is_known() {
        assert_eq!(unread_earlier_note(Some(EVENT_COMMENTS_SENT)), None);
        // A retry from the dashboard on a ticket no event marked.
        assert_eq!(unread_earlier_note(None), None);
        assert_eq!(unread_earlier_note(Some("")), None);
    }

    #[test]
    fn open_comments_are_oldest_first_and_bounded() {
        let mut all: Vec<Comment> = (0..30).map(|i| comment(&format!("{i}"), "x")).collect();
        all[0].state = State::Handled;
        all.reverse();
        let o = open(all);
        assert_eq!(o.len(), MAX_BURST);
        assert!(o.iter().all(Comment::is_open));
        assert!(o.windows(2).all(|w| w[0].at <= w[1].at));
    }
}
