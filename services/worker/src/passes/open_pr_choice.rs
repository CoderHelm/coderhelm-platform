//! What to do about a pull request someone else already opened for the
//! ticket: work on it, or open CoderHelm's own.
//!
//! CoderHelm never opens a second pull request for a ticket on its own. It
//! posts a notice and waits. The person's answer can come as a reply on the
//! ticket (Jira) or the issue (GitHub), or as an answer on the run page. Every
//! run reads those answers here, newest first, has the model say which choice
//! each one makes (structured output: `own`, `existing` or `unclear`), and
//! records the first clear one on the ticket's record
//! (`ticket_state::Decision`), so every later run honours it without asking
//! again. Closing the other pull request is never CoderHelm's call: `own`
//! leaves it open.

use crate::agent::provider::ModelProvider;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::passes::attr_s;
use crate::WorkerState;
use common::run_questions::{Answer, Question};
use common::ticket_state::{self, Decision, Note, Notice, CHOICE_EXISTING, CHOICE_OWN};
use serde_json::{json, Value};
use tracing::{info, warn};

/// Most replies read for one decision.
const MAX_REPLIES: usize = 6;

/// A pull request for the ticket that isn't CoderHelm's own for this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtherPr {
    /// `owner/name`.
    pub repo: String,
    pub number: u64,
    pub title: String,
    pub author: String,
    /// Its branch.
    pub head: String,
    /// Its branch is in `repo` itself (not a fork): CoderHelm can push to it.
    pub same_repo: bool,
}

impl OtherPr {
    /// `owner/name#number`.
    pub fn subject(&self) -> String {
        format!("{}#{}", self.repo, self.number)
    }

    /// The decision key questions and notices carry.
    pub fn key(&self) -> String {
        ticket_state::decision_key(ticket_state::KIND_OPEN_PR, &self.subject())
    }
}

/// What a person chose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Choice {
    Own,
    Existing,
    Unclear,
}

impl Choice {
    pub fn parse(s: &str) -> Choice {
        match s.trim().to_ascii_lowercase().as_str() {
            "own" => Choice::Own,
            "existing" => Choice::Existing,
            _ => Choice::Unclear,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Choice::Own => CHOICE_OWN,
            Choice::Existing => CHOICE_EXISTING,
            Choice::Unclear => "unclear",
        }
    }
}

/// The notice CoderHelm posts, word for word.
pub fn notice_text(ticket_id: &str, pr: &OtherPr) -> String {
    format!(
        "An open pull request for {ticket_id} already exists in {}: #{} \"{}\"{}. I didn't open \
         another. Reply here to have me work on that pull request, or to go ahead with my own.",
        pr.repo,
        pr.number,
        pr.title,
        if pr.author.is_empty() {
            String::new()
        } else {
            format!(" by {}", pr.author)
        }
    )
}

/// The notice as a question on the run page: two choices as buttons.
pub fn notice_question(pr: &OtherPr) -> Question {
    Question {
        id: "q1".to_string(),
        question: format!(
            "A pull request is already open: work on #{}, or open my own?",
            pr.number
        ),
        why: format!(
            "#{} \"{}\" in {} already names this ticket, so I didn't open another. Opening my \
             own leaves #{} open as it is.",
            pr.number, pr.title, pr.repo, pr.number
        ),
        suggestions: vec![
            choice_label(Choice::Existing, pr),
            choice_label(Choice::Own, pr),
        ],
        decision: pr.key(),
    }
}

/// The words of a choice's button.
pub fn choice_label(choice: Choice, pr: &OtherPr) -> String {
    match choice {
        Choice::Existing => format!("Work on #{}", pr.number),
        _ => "Open my own".to_string(),
    }
}

/// One answer to the notice, from wherever it was given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub author: String,
    pub text: String,
    /// RFC 3339.
    pub at: String,
}

/// The answers to the notice about `pr`, newest first: answers on the run
/// page to the notice's question, and replies people wrote on the ticket
/// (Jira `notes`) or the issue (GitHub `comments`) after the notice was
/// posted (`since`; every reply when unknown, as for a notice posted before
/// notices were recorded).
pub fn replies(
    key: &str,
    answers: &[Answer],
    notes: &[Note],
    comments: &[Value],
    since: Option<&str>,
) -> Vec<Reply> {
    let after = |at: &str| since.is_none_or(|s| at > s);
    let mut out: Vec<Reply> = Vec::new();
    for a in answers.iter().filter(|a| a.decision == key) {
        out.push(Reply {
            author: a.author.clone(),
            text: a.answer.clone(),
            at: a.answered_at.clone(),
        });
    }
    for n in notes.iter().filter(|n| after(&n.at)) {
        out.push(Reply {
            author: n.author.clone(),
            text: n.text.clone(),
            at: n.at.clone(),
        });
    }
    for c in comments {
        let login = c["user"]["login"].as_str().unwrap_or("");
        let bot = c["user"]["type"].as_str() == Some("Bot") || super::is_own_login(login);
        let at = c["created_at"].as_str().unwrap_or("");
        let text = c["body"].as_str().unwrap_or("").trim();
        if !bot && !text.is_empty() && after(at) {
            out.push(Reply {
                author: login.to_string(),
                text: text.to_string(),
                at: at.to_string(),
            });
        }
    }
    out.sort_by(|a, b| b.at.cmp(&a.at));
    out.truncate(MAX_REPLIES);
    out
}

pub const SYSTEM: &str = "CoderHelm, an automated coding agent, found a pull request someone \
     else already opened for a ticket, so it did not open its own and posted a notice asking \
     what to do. You read one reply a person wrote after that notice and say which choice it \
     makes. own: CoderHelm should go ahead and open its own pull request (\"go ahead\", \"open \
     your own\", \"ignore that one\", \"that PR is stale, start fresh\"). existing: CoderHelm \
     should work on, update or take over the existing pull request. unclear: the reply doesn't \
     choose (it is about something else, asks a question, or is ambiguous). Judge the reply's \
     meaning, not its words. Closing the other pull request is not part of either choice.";

pub fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "choice": {"type": "string", "enum": ["own", "existing", "unclear"]},
            "reason": {"type": "string"}
        },
        "required": ["choice", "reason"],
        "additionalProperties": false
    })
}

pub fn prompt(notice: &str, reply: &Reply) -> String {
    format!(
        "## CoderHelm's notice\n{notice}\n\n## The reply (by {})\n{}\n",
        if reply.author.trim().is_empty() {
            "a person"
        } else {
            reply.author.trim()
        },
        common::truncate_str(reply.text.trim(), 4000)
    )
}

/// The model's reading of one reply; `Unclear` when it couldn't be read.
async fn classify(
    state: &WorkerState,
    provider: &ModelProvider,
    notice: &str,
    reply: &Reply,
    usage: &mut TokenUsage,
) -> Choice {
    match crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        SYSTEM,
        &prompt(notice, reply),
        &schema(),
        true,
        "low",
        512,
        usage,
    )
    .await
    {
        Ok(v) => {
            let c = Choice::parse(v["choice"].as_str().unwrap_or(""));
            info!(
                choice = c.as_str(),
                reason = v["reason"].as_str().unwrap_or(""),
                "Read a reply to the open pull request notice"
            );
            c
        }
        Err(e) => {
            warn!(error = %e, "Could not read a reply to the open pull request notice");
            Choice::Unclear
        }
    }
}

/// The ticket's record: decisions and notices CoderHelm keeps on it.
#[derive(Debug, Clone, Default)]
pub struct Record {
    pub decisions: Vec<Decision>,
    pub notices: Vec<Notice>,
    pub notes: Vec<Note>,
}

pub async fn load_record(state: &WorkerState, team_id: &str, ticket_id: &str) -> Record {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&ticket_state::sk(ticket_id)))
        .consistent_read(true)
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let field = |k: &str| {
        item.as_ref()
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    Record {
        decisions: ticket_state::parse_decisions(&field(ticket_state::FIELD_DECISIONS)),
        notices: ticket_state::parse_notices(&field(ticket_state::FIELD_NOTICES)),
        notes: ticket_state::parse_notes(&field(ticket_state::FIELD_NOTES)),
    }
}

async fn save_field(
    state: &WorkerState,
    team_id: &str,
    ticket_id: &str,
    field: &str,
    json: String,
) {
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&ticket_state::sk(ticket_id)))
        .update_expression("SET #f = :v, updated_at = :t")
        .expression_attribute_names("#f", field)
        .expression_attribute_values(":v", attr_s(&json))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
    {
        warn!(ticket_id, field, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not update the ticket's record");
    }
}

/// Records a decision on the ticket, in place of an earlier one on the
/// same subject.
pub async fn record_decision(state: &WorkerState, team_id: &str, ticket_id: &str, d: Decision) {
    let rec = load_record(state, team_id, ticket_id).await;
    let all = ticket_state::with_decision(rec.decisions, d);
    save_field(
        state,
        team_id,
        ticket_id,
        ticket_state::FIELD_DECISIONS,
        serde_json::to_string(&all).unwrap_or_default(),
    )
    .await;
}

/// Records a notice posted on the ticket.
pub async fn record_notice(state: &WorkerState, team_id: &str, ticket_id: &str, n: Notice) {
    let rec = load_record(state, team_id, ticket_id).await;
    let all = ticket_state::with_notice(rec.notices, n);
    save_field(
        state,
        team_id,
        ticket_id,
        ticket_state::FIELD_NOTICES,
        serde_json::to_string(&all).unwrap_or_default(),
    )
    .await;
}

/// What a person decided about `pr`: the recorded decision, else the first
/// clear answer among the replies to the notice (newest first), which is
/// then recorded. `Unclear` when nobody has chosen.
pub async fn decide(
    state: &WorkerState,
    provider: &ModelProvider,
    msg: &TicketMessage,
    github: &crate::clients::github::GitHubClient,
    pr: &OtherPr,
    usage: &mut TokenUsage,
) -> Choice {
    let key = pr.key();
    let rec = load_record(state, &msg.team_id, &msg.ticket_id).await;
    if let Some(d) = ticket_state::decision_on(&rec.decisions, &key) {
        return Choice::parse(&d.choice);
    }
    let since = rec
        .notices
        .iter()
        .find(|n| n.key == key)
        .map(|n| n.at.clone());
    let comments = if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        github
            .list_issue_comments(&msg.repo_owner, &msg.repo_name, msg.issue_number)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let notes = if matches!(msg.source, TicketSource::Jira) {
        rec.notes
    } else {
        Vec::new()
    };
    let notice = notice_text(&msg.ticket_id, pr);
    for reply in replies(&key, &msg.answers, &notes, &comments, since.as_deref()) {
        let choice = classify(state, provider, &notice, &reply, usage).await;
        if choice == Choice::Unclear {
            continue;
        }
        record_decision(
            state,
            &msg.team_id,
            &msg.ticket_id,
            Decision {
                kind: ticket_state::KIND_OPEN_PR.to_string(),
                subject: pr.subject(),
                choice: choice.as_str().to_string(),
                said: common::truncate_str(reply.text.trim(), 600).to_string(),
                by: reply.author,
                at: chrono::Utc::now().to_rfc3339(),
            },
        )
        .await;
        return choice;
    }
    Choice::Unclear
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::ticket_state::Intent;

    fn pr() -> OtherPr {
        OtherPr {
            repo: "acme/web".into(),
            number: 12,
            title: "Fix the sign-up form".into(),
            author: "a-maintainer".into(),
            head: "fix/sign-up".into(),
            same_repo: true,
        }
    }

    fn note(text: &str, at: &str) -> Note {
        Note {
            author: "A Teammate".into(),
            text: text.into(),
            intent: Intent::Other,
            at: at.into(),
        }
    }

    #[test]
    fn the_notice_becomes_a_choice_on_the_run_page() {
        let q = notice_question(&pr());
        assert_eq!(
            q.question,
            "A pull request is already open: work on #12, or open my own?"
        );
        assert_eq!(q.suggestions, vec!["Work on #12", "Open my own"]);
        assert_eq!(q.decision, "open_pr:acme/web#12");
        assert!(q.why.contains("leaves #12 open"));
        assert!(
            notice_text("ABC-7", &pr()).contains("#12 \"Fix the sign-up form\" by a-maintainer")
        );
    }

    #[test]
    fn replies_are_read_newest_first_and_only_after_the_notice() {
        let answers = vec![
            Answer {
                question_id: "q1".into(),
                question: "A pull request is already open: work on #12, or open my own?".into(),
                answer: "Open my own".into(),
                author: "A Teammate".into(),
                answered_at: "2026-10-27T12:00:00Z".into(),
                decision: "open_pr:acme/web#12".into(),
            },
            // An answer to another question is not a reply to this notice.
            Answer {
                question_id: "q1".into(),
                question: "Which page?".into(),
                answer: "Pricing".into(),
                answered_at: "2026-10-27T13:00:00Z".into(),
                ..Default::default()
            },
        ];
        let notes = vec![
            note("Before the notice", "2026-10-27T08:00:00Z"),
            note(
                "@coderhelm Go ahead and open your own",
                "2026-10-27T11:00:00Z",
            ),
        ];
        let comments = vec![
            json!({"user": {"login": "coderhelm[bot]", "type": "Bot"}, "body": "notice", "created_at": "2026-10-27T10:00:00Z"}),
            json!({"user": {"login": "someone", "type": "User"}, "body": "work on that one", "created_at": "2026-10-27T10:30:00Z"}),
        ];
        let r = replies(
            "open_pr:acme/web#12",
            &answers,
            &notes,
            &comments,
            Some("2026-10-27T09:00:00Z"),
        );
        let texts: Vec<&str> = r.iter().map(|r| r.text.as_str()).collect();
        assert_eq!(
            texts,
            vec![
                "Open my own",
                "@coderhelm Go ahead and open your own",
                "work on that one"
            ]
        );
        // No notice on record (posted before notices were kept): every reply.
        assert_eq!(
            replies("open_pr:acme/web#12", &[], &notes, &[], None).len(),
            2
        );
    }

    #[test]
    fn the_classifier_is_asked_for_one_of_three_choices() {
        assert_eq!(
            schema()["properties"]["choice"]["enum"],
            json!(["own", "existing", "unclear"])
        );
        let p = prompt(
            &notice_text("ABC-7", &pr()),
            &Reply {
                author: "".into(),
                text: "Go ahead and open your own".into(),
                at: String::new(),
            },
        );
        assert!(p.contains("## The reply (by a person)\nGo ahead and open your own"));
        assert_eq!(Choice::parse("Own"), Choice::Own);
        assert_eq!(Choice::parse("existing"), Choice::Existing);
        assert_eq!(Choice::parse("maybe"), Choice::Unclear);
    }
}
