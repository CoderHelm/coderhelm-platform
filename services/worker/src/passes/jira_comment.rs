//! A person's comment on a ticket CoderHelm worked on, read before anything
//! runs.
//!
//! Every comment used to start work: a new run on a ticket without an open
//! PR, feedback (often a code change) on one with. A comment that only says
//! "fixed this in the booking system" or "moved to QA" started the same run
//! again. The model now reads the comment first (structured output, metered
//! as feedback) as one of [`Intent`]:
//!
//! - `new_requirement`: the work the gateway prepared goes ahead.
//! - `question`: answered on the open PR (feedback); never a new code run.
//! - `fix_elsewhere_info`: the lesson is recorded and the ticket is marked
//!   done for CoderHelm — no run. With an open PR, feedback reads it and
//!   closes that PR.
//! - `status_only` / `other`: nothing runs.
//!
//! Every comment read is kept on the ticket's record, so the next plan sees
//! what people wrote.

use crate::agent::provider::ModelProvider;
use crate::models::{JiraCommentMessage, TokenUsage, WorkerMessage};
use crate::passes::attr_s;
use crate::WorkerState;
use common::ticket_state::{self, Intent, Note};
use serde_json::json;
use tracing::{info, warn};

/// What the comment leads to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Act {
    /// Send the prepared action (run or feedback).
    Dispatch,
    /// The fix was made or belongs elsewhere: record it and mark the ticket
    /// done; `and_feedback` also sends the feedback (an open PR to close).
    Elsewhere {
        and_feedback: bool,
    },
    Nothing,
}

/// What the prepared action is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    Feedback,
    Run,
    None,
}

fn action_kind(a: Option<&WorkerMessage>) -> ActionKind {
    match a {
        Some(WorkerMessage::Feedback(_)) => ActionKind::Feedback,
        Some(WorkerMessage::Ticket(_)) => ActionKind::Run,
        _ => ActionKind::None,
    }
}

/// Whether a comment that starts a run is a person's direction for it: it
/// asks for the work, or it answers CoderHelm's own question on a waiting
/// run (whatever words it uses).
pub fn is_direction(intent: Option<Intent>, run_status: &str) -> bool {
    let waiting_for_answer = matches!(
        run_status,
        "needs_input" | "needs_outside_change" | "failed"
    );
    match intent {
        Some(Intent::NewRequirement) => true,
        None | Some(Intent::Other) => waiting_for_answer,
        _ => false,
    }
}

/// The decision. `intent` is `None` when the model couldn't read the comment:
/// feedback on an open PR (which reads it again) and an answer to
/// CoderHelm's own question still go ahead; nothing else starts a run.
pub fn decide(intent: Option<Intent>, run_status: &str, action: ActionKind) -> Act {
    let waiting_for_answer = matches!(run_status, "needs_input" | "failed");
    match (intent, action) {
        (_, ActionKind::None) => match intent {
            Some(Intent::FixElsewhereInfo) => Act::Elsewhere {
                and_feedback: false,
            },
            _ => Act::Nothing,
        },
        (None, ActionKind::Feedback) => Act::Dispatch,
        (None, ActionKind::Run) if waiting_for_answer => Act::Dispatch,
        (None, ActionKind::Run) => Act::Nothing,
        (Some(Intent::NewRequirement), _) => Act::Dispatch,
        (Some(Intent::Question), ActionKind::Feedback) => Act::Dispatch,
        (Some(Intent::Question), _) => Act::Nothing,
        (Some(Intent::FixElsewhereInfo), kind) => Act::Elsewhere {
            and_feedback: kind == ActionKind::Feedback,
        },
        (Some(Intent::StatusOnly | Intent::Other), _) => Act::Nothing,
    }
}

pub const SYSTEM: &str = "You read one comment a person wrote on a ticket that an automated \
     coding agent (CoderHelm) already worked on, and say what the comment is. \
     new_requirement: it asks for new or different work in the code, or answers the agent's \
     question with details it needs to do the work. \
     fix_elsewhere_info: it says the issue was fixed, or belongs, somewhere other than the code \
     (a setting or data in another system, a manual change), or that it is resolved or no longer \
     needed. \
     question: it asks something. \
     status_only: it only reports progress or status (\"looking into it\", \"moved to QA\"). \
     other: anything else (thanks, a mention, an unrelated note). \
     Judge the comment as a whole; a comment that says the fix was made elsewhere AND asks for a \
     code change is new_requirement.";

pub fn schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "intent": {"type": "string", "enum": ["new_requirement", "fix_elsewhere_info", "question", "status_only", "other"]},
            "reason": {"type": "string"}
        },
        "required": ["intent", "reason"],
        "additionalProperties": false
    })
}

pub fn prompt(msg: &JiraCommentMessage) -> String {
    let state = match msg.run_status.as_str() {
        "needs_input" => "CoderHelm asked a question on the ticket and is waiting for an answer.",
        "failed" => "CoderHelm's last run on the ticket failed.",
        "needs_outside_change" => {
            "CoderHelm reported that the fix is a change outside the code and opened no PR."
        }
        "completed" | "success" if msg.pr_number > 0 => {
            "CoderHelm opened a pull request for the ticket."
        }
        "merged" => "CoderHelm's pull request for the ticket was merged.",
        "cancelled" => "CoderHelm's work on the ticket was stopped (or its PR closed).",
        _ => "CoderHelm finished its last run on the ticket.",
    };
    format!(
        "## Ticket\n{}\n\n## CoderHelm's state\n{state}\n\n## The comment (by {})\n{}\n",
        msg.title.trim(),
        if msg.author.trim().is_empty() {
            "a person"
        } else {
            msg.author.trim()
        },
        common::truncate_str(msg.comment.trim(), 4000)
    )
}

/// The model's reading of the comment; `None` when it couldn't be read.
async fn classify(
    state: &WorkerState,
    provider: &ModelProvider,
    msg: &JiraCommentMessage,
    usage: &mut TokenUsage,
) -> Option<Intent> {
    match crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        SYSTEM,
        &prompt(msg),
        &schema(),
        true,
        "low",
        512,
        usage,
    )
    .await
    {
        Ok(v) => {
            let intent = Intent::parse(v["intent"].as_str().unwrap_or(""));
            info!(
                ticket_id = %msg.ticket_id,
                intent = intent.as_str(),
                reason = v["reason"].as_str().unwrap_or(""),
                "Read the ticket comment"
            );
            Some(intent)
        }
        Err(e) => {
            warn!(ticket_id = %msg.ticket_id, error = %e, "Could not read the ticket comment");
            None
        }
    }
}

/// Keeps the comment on the ticket's record (and marks it done, when
/// `done_reason` is set).
async fn record(
    state: &WorkerState,
    msg: &JiraCommentMessage,
    note: Note,
    done_reason: Option<&str>,
) {
    let sk = ticket_state::sk(&msg.ticket_id);
    let table = &state.config.settings_table_name;
    let mut notes = state
        .dynamo
        .get_item()
        .table_name(table)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| {
            i.get(ticket_state::FIELD_NOTES)
                .and_then(|v| v.as_s().ok())
                .cloned()
        })
        .map(|j| ticket_state::parse_notes(&j))
        .unwrap_or_default();
    notes.push(note);
    if notes.len() > ticket_state::MAX_NOTES {
        let drop = notes.len() - ticket_state::MAX_NOTES;
        notes.drain(..drop);
    }
    let now = chrono::Utc::now().to_rfc3339();
    let mut up = state
        .dynamo
        .update_item()
        .table_name(table)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&sk))
        .expression_attribute_names("#n", ticket_state::FIELD_NOTES)
        .expression_attribute_values(
            ":n",
            attr_s(&serde_json::to_string(&notes).unwrap_or_default()),
        )
        .expression_attribute_values(":t", attr_s(&now));
    up = match done_reason {
        Some(reason) => up
            .update_expression("SET #n = :n, updated_at = :t, #s = :s, #r = :r, #h = :h")
            .expression_attribute_names("#s", ticket_state::FIELD_STATE)
            .expression_attribute_names("#r", ticket_state::FIELD_DONE_REASON)
            .expression_attribute_names("#h", ticket_state::FIELD_DONE_HASH)
            .expression_attribute_values(":s", attr_s(ticket_state::STATE_DONE))
            .expression_attribute_values(":r", attr_s(reason))
            .expression_attribute_values(":h", attr_s(&msg.context_hash)),
        None => up.update_expression("SET #n = :n, updated_at = :t"),
    };
    if let Err(e) = up.send().await {
        warn!(ticket_id = %msg.ticket_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not record the ticket comment");
    }
}

/// Sends the prepared action: a run (under the ticket lock, so two
/// comments can't start two runs) or feedback on the open PR.
/// The comment's action carrying the person's direction onto the ticket
/// run it starts (see `TicketMessage::person_direction`); `None` when there
/// is no direction or the action isn't a ticket run.
fn with_direction(action: &WorkerMessage, direction: Option<String>) -> Option<WorkerMessage> {
    match (action, direction) {
        (WorkerMessage::Ticket(t), Some(d)) => {
            let mut t = t.clone();
            t.person_direction = Some(d);
            Some(WorkerMessage::Ticket(t))
        }
        _ => None,
    }
}

async fn dispatch(state: &WorkerState, msg: &JiraCommentMessage, action: &WorkerMessage) -> bool {
    let queue = match action {
        WorkerMessage::Feedback(_) => &state.config.feedback_queue_url,
        _ => {
            if !super::acquire_ticket_lock(state, &msg.team_id, &msg.ticket_id).await {
                info!(ticket_id = %msg.ticket_id, "Ticket lock held — not starting another run");
                return false;
            }
            &state.config.ticket_queue_url
        }
    };
    if queue.is_empty() {
        warn!(ticket_id = %msg.ticket_id, "No queue configured for the comment's action");
        return false;
    }
    let body = match serde_json::to_string(action) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "Could not serialize the comment's action");
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
            warn!(ticket_id = %msg.ticket_id, error = %aws_sdk_sqs::error::DisplayErrorContext(&e), "Could not send the comment's action");
            false
        }
    }
}

pub async fn run(
    state: &WorkerState,
    msg: JiraCommentMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await?;
    let mut usage = TokenUsage::default();
    let intent = classify(state, &provider, &msg, &mut usage).await;
    let kind = action_kind(msg.action.as_deref());
    let act = decide(intent, &msg.run_status, kind);
    info!(
        ticket_id = %msg.ticket_id,
        run_status = %msg.run_status,
        intent = intent.map(Intent::as_str).unwrap_or("unread"),
        act = ?act,
        "Ticket comment decision"
    );
    let note = Note {
        author: msg.author.clone(),
        text: common::truncate_str(msg.comment.trim(), 2000).to_string(),
        intent: intent.unwrap_or(Intent::Other),
        at: chrono::Utc::now().to_rfc3339(),
    };
    let direction = is_direction(intent, &msg.run_status)
        .then(|| format!("{}: {}", note.author, common::truncate_str(&note.text, 600)));
    match act {
        Act::Dispatch => {
            record(state, &msg, note, None).await;
            if let Some(a) = msg.action.as_deref() {
                let directed = with_direction(a, direction);
                dispatch(state, &msg, directed.as_ref().unwrap_or(a)).await;
            }
        }
        Act::Elsewhere { and_feedback } => {
            record(
                state,
                &msg,
                note,
                Some("A person wrote that the fix was made, or belongs, outside the code"),
            )
            .await;
            if and_feedback {
                // Feedback reads it on the open PR: lesson, then the PR closes.
                if let Some(a) = msg.action.as_deref() {
                    dispatch(state, &msg, a).await;
                }
            } else {
                super::lessons::learn_from_ticket_comment(state, &provider, &msg, &mut usage).await;
            }
        }
        Act::Nothing => record(state, &msg, note, None).await,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_informational_comment_never_starts_a_run() {
        // "Fixed it in the booking system" on a ticket without an open PR.
        for status in [
            "completed",
            "needs_outside_change",
            "cancelled",
            "needs_input",
        ] {
            assert_eq!(
                decide(Some(Intent::FixElsewhereInfo), status, ActionKind::Run),
                Act::Elsewhere {
                    and_feedback: false
                }
            );
        }
        // With CoderHelm's PR open, feedback reads it and closes the PR.
        assert_eq!(
            decide(
                Some(Intent::FixElsewhereInfo),
                "completed",
                ActionKind::Feedback
            ),
            Act::Elsewhere { and_feedback: true }
        );
    }

    #[test]
    fn status_and_chatter_start_nothing() {
        for kind in [ActionKind::Run, ActionKind::Feedback] {
            assert_eq!(
                decide(Some(Intent::StatusOnly), "completed", kind),
                Act::Nothing
            );
            assert_eq!(
                decide(Some(Intent::Other), "needs_input", kind),
                Act::Nothing
            );
        }
        assert_eq!(
            decide(Some(Intent::Question), "completed", ActionKind::Run),
            Act::Nothing
        );
    }

    #[test]
    fn real_work_still_goes_ahead() {
        assert_eq!(
            decide(Some(Intent::NewRequirement), "completed", ActionKind::Run),
            Act::Dispatch
        );
        assert_eq!(
            decide(Some(Intent::Question), "completed", ActionKind::Feedback),
            Act::Dispatch
        );
        // The model couldn't read it: an answer to CoderHelm's question and
        // PR feedback still go ahead; nothing else starts a code run.
        assert_eq!(decide(None, "needs_input", ActionKind::Run), Act::Dispatch);
        assert_eq!(
            decide(None, "completed", ActionKind::Feedback),
            Act::Dispatch
        );
        assert_eq!(decide(None, "completed", ActionKind::Run), Act::Nothing);
    }

    #[test]
    fn the_message_round_trips_with_its_action() {
        let v = serde_json::json!({
            "type": "jira_comment",
            "team_id": "TEAM#1", "installation_id": 7, "ticket_id": "ABC-1",
            "title": "Class missing", "run_id": "01A", "run_status": "completed",
            "pr_number": 0, "repo_owner": "acme", "repo_name": "pipelines",
            "author": "a person", "comment": "Fixed in the booking system.",
            "context_hash": "h1",
            "action": {"type": "ticket", "team_id": "TEAM#1", "installation_id": 7,
                "source": "jira", "ticket_id": "ABC-1", "title": "Class missing",
                "body": "b", "repo_owner": "", "repo_name": "", "issue_number": 0,
                "sender": "a person", "image_attachments": []}
        });
        let m: WorkerMessage = serde_json::from_value(v).unwrap();
        let WorkerMessage::JiraComment(c) = m else {
            panic!("not a jira comment")
        };
        assert_eq!(action_kind(c.action.as_deref()), ActionKind::Run);
        assert!(prompt(&c).contains("Fixed in the booking system."));
    }
}

#[cfg(test)]
mod direction_tests {
    use super::*;

    #[test]
    fn an_answer_to_a_waiting_run_is_a_direction() {
        assert!(is_direction(Some(Intent::NewRequirement), "completed"));
        assert!(
            is_direction(None, "needs_input"),
            "unread answer to our question"
        );
        assert!(is_direction(Some(Intent::Other), "needs_outside_change"));
        assert!(!is_direction(None, "completed"));
        assert!(!is_direction(Some(Intent::Question), "needs_input"));
        assert!(!is_direction(Some(Intent::StatusOnly), "needs_input"));
        assert!(!is_direction(Some(Intent::FixElsewhereInfo), "needs_input"));
    }
}
