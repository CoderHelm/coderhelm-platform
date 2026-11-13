//! A person's comment on a ticket, taken in one way whatever the ticket's
//! source (see `common::ticket_comments`).
//!
//! The webhooks used to drop a comment that arrived while a run was
//! working ("Ignoring Jira comment — run already in progress", "Skipping
//! slash command — run already in flight", and a comment on a GitHub issue
//! that didn't mention CoderHelm). They now call [`receive`]:
//!
//! 1. the comment is kept, once per comment id (a webhook delivered twice
//!    is one comment);
//! 2. the first time it is seen while a run works, it is acknowledged on
//!    the ticket: a reaction on GitHub, one short reply in Jira. Never for
//!    CoderHelm's own comments or another bot's;
//! 3. the run page gets a note with what the person wrote;
//! 4. the worker is told at once, so a comment that says to stop is read
//!    now, not at the run's next step.
//!
//! A comment on a ticket no run is working on is kept ([`keep`], with no
//! acknowledgement) only when CoderHelm has something to do with the
//! ticket: it carries the team's trigger label or is assigned to
//! CoderHelm. Comments on every other ticket are never stored. When a run
//! starts on such a ticket, the event that starts it brings the ticket's
//! comments ([`keep_jira_existing`]), and a GitHub run reads the issue's.

use crate::models::{JiraCommentMessage, WorkerMessage};
use crate::AppState;
use aws_sdk_dynamodb::types::AttributeValue;
use axum::http::StatusCode;
use common::ticket_comments::{self as tc, Ack, Comment, Source, Store};
use serde_json::{json, Value};
use tracing::{info, warn};

/// A comment as a webhook delivered it.
#[derive(Debug, Clone, Default)]
pub struct Incoming<'a> {
    pub team_id: &'a str,
    pub installation_id: u64,
    pub source: Source,
    pub ticket_id: &'a str,
    /// The tracker's id for the comment.
    pub comment_id: &'a str,
    pub author: &'a str,
    /// The tracker's account type for the author (`Bot`, `app`, ...).
    pub author_kind: &'a str,
    pub text: &'a str,
    pub title: &'a str,
    /// The ticket's latest run and its status; empty when it has none.
    pub run_id: &'a str,
    pub run_status: &'a str,
    pub pr_number: u64,
    pub repo_owner: &'a str,
    pub repo_name: &'a str,
}

/// What became of an incoming comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Received {
    /// CoderHelm's own comment or another bot's: nothing is kept.
    NotAPerson,
    /// Already kept (the same delivery again): nothing more happens.
    Duplicate,
    /// Kept now, with how it is acknowledged.
    Kept(Box<Comment>, Ack),
}

/// Keeps the comment and says how it is acknowledged. No side effect
/// beyond the store: [`receive`] does the acknowledgement.
pub async fn take(store: &dyn Store, inc: &Incoming<'_>) -> Result<Received, String> {
    if !tc::is_person(inc.source, inc.author, inc.author_kind, inc.text) {
        return Ok(Received::NotAPerson);
    }
    let working = tc::run_working(inc.run_status) && !inc.run_id.is_empty();
    let comment = Comment {
        ticket_id: inc.ticket_id.to_string(),
        source: inc.source,
        comment_id: tc::comment_id(inc.comment_id, inc.author, inc.text),
        author: inc.author.trim().to_string(),
        text: common::truncate_str(inc.text.trim(), tc::MAX_TEXT_CHARS).to_string(),
        at: chrono::Utc::now().to_rfc3339(),
        run_id: if working {
            inc.run_id.to_string()
        } else {
            String::new()
        },
        ..Default::default()
    };
    let new = store.record(inc.team_id, &comment).await?;
    if !new {
        return Ok(Received::Duplicate);
    }
    let ack = tc::ack_for(inc.source, true, new, working);
    Ok(Received::Kept(Box::new(comment), ack))
}

fn store(state: &AppState) -> tc::Dynamo<'_> {
    tc::Dynamo {
        client: &state.dynamo,
        table: &state.config.settings_table_name,
    }
}

/// A comment on a ticket a run is working on: kept, acknowledged, shown on
/// the run, and sent to the worker to read. Returns what became of it.
pub async fn receive(state: &AppState, inc: &Incoming<'_>) -> Result<Received, StatusCode> {
    let received = take(&store(state), inc).await.map_err(|e| {
        // The webhook is delivered again: the comment is never lost here.
        warn!(ticket_id = inc.ticket_id, error = %e, "Could not keep the ticket comment");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let Received::Kept(comment, ack) = &received else {
        info!(
            ticket_id = inc.ticket_id,
            outcome = ?received,
            "Ticket comment not taken again"
        );
        return Ok(received);
    };
    info!(
        team_id = inc.team_id,
        ticket_id = inc.ticket_id,
        run_id = inc.run_id,
        run_status = inc.run_status,
        ack = ?ack,
        "Ticket comment kept for the run working on the ticket"
    );
    match ack {
        Ack::Reaction => react(state, inc).await,
        Ack::Reply => reply(state, inc).await,
        Ack::None => {}
    }
    if !comment.run_id.is_empty() {
        note(
            state,
            inc.team_id,
            &comment.run_id,
            &tc::received_note(comment, *ack),
        )
        .await;
        // The worker reads it now: a comment that says to stop ends the run
        // at once, and a run that just finished gets it as a round.
        let message = WorkerMessage::JiraComment(JiraCommentMessage {
            team_id: inc.team_id.to_string(),
            installation_id: inc.installation_id,
            ticket_id: inc.ticket_id.to_string(),
            title: inc.title.to_string(),
            run_id: inc.run_id.to_string(),
            run_status: inc.run_status.to_string(),
            pr_number: inc.pr_number,
            repo_owner: inc.repo_owner.to_string(),
            repo_name: inc.repo_name.to_string(),
            author: comment.author.clone(),
            comment: comment.text.clone(),
            context_hash: String::new(),
            action: None,
        });
        match serde_json::to_string(&message) {
            Ok(body) => {
                if let Err(e) = state
                    .sqs
                    .send_message()
                    .queue_url(&state.config.ticket_queue_url)
                    .message_body(body)
                    .send()
                    .await
                {
                    // Kept all the same: the run reads it at its next step.
                    warn!(ticket_id = inc.ticket_id, error = %e, "Could not send the ticket comment to the worker");
                }
            }
            Err(e) => warn!(error = %e, "Could not serialize the ticket comment"),
        }
    }
    Ok(received)
}

/// Keeps a comment written while no run works on the ticket, as context
/// for the run that starts on it. No acknowledgement and no note.
pub async fn keep(state: &AppState, inc: &Incoming<'_>) {
    let quiet = Incoming {
        run_id: "",
        run_status: "",
        ..inc.clone()
    };
    match take(&store(state), &quiet).await {
        Ok(Received::Kept(..)) => info!(
            ticket_id = inc.ticket_id,
            "Ticket comment kept for the run that starts on the ticket"
        ),
        Ok(_) => {}
        Err(e) => {
            warn!(ticket_id = inc.ticket_id, error = %e, "Could not keep the ticket comment")
        }
    }
}

/// The comments a Jira event carries with the ticket (`comments`: id,
/// author, body), kept as context for the run the event starts: this is
/// how a run reads what people wrote on a ticket whose comments were
/// never stored. The ticket's record says whether the event carried them,
/// so a run started by an older Jira app says they were not read.
pub async fn keep_jira_existing(state: &AppState, team_id: &str, ticket_id: &str, payload: &Value) {
    let kept = keep_existing_in(&store(state), team_id, ticket_id, payload).await;
    let sent = event_carries_comments(payload);
    info!(
        ticket_id,
        sent, kept, "The ticket's existing comments, from the event that starts the run"
    );
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(common::ticket_state::sk(ticket_id)))
        .update_expression("SET #f = :v, updated_at = :t")
        .expression_attribute_names("#f", tc::FIELD_EVENT_COMMENTS)
        .expression_attribute_values(
            ":v",
            AttributeValue::S(
                if sent {
                    tc::EVENT_COMMENTS_SENT
                } else {
                    tc::EVENT_COMMENTS_NOT_SENT
                }
                .to_string(),
            ),
        )
        .expression_attribute_values(":t", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
        .send()
        .await
    {
        warn!(ticket_id, error = %e, "Could not note whether the event carried the ticket's comments");
    }
}

/// Whether a Jira event carries the ticket's comments at all (an empty
/// list is a ticket without comments; a missing one is an app version
/// that does not send them).
pub fn event_carries_comments(payload: &Value) -> bool {
    payload["comments"].is_array()
}

/// Keeps the people's comments an event carries, as context (no run was
/// working). Returns how many were newly kept.
pub async fn keep_existing_in(
    store: &dyn Store,
    team_id: &str,
    ticket_id: &str,
    payload: &Value,
) -> usize {
    let mut kept = 0;
    for c in existing_jira_comments(payload) {
        let inc = Incoming {
            team_id,
            source: Source::Jira,
            ticket_id,
            comment_id: &c.id,
            author: &c.author,
            author_kind: &c.kind,
            text: &c.text,
            ..Default::default()
        };
        match take(store, &inc).await {
            Ok(Received::Kept(..)) => kept += 1,
            Ok(_) => {}
            Err(e) => warn!(ticket_id, error = %e, "Could not keep a ticket comment"),
        }
    }
    kept
}

/// One comment a Jira event carries with its ticket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Existing {
    pub id: String,
    pub author: String,
    pub kind: String,
    pub text: String,
}

/// Most comments read from a ticket when a run starts on it (the newest).
pub const MAX_EXISTING: usize = 20;

/// The ticket's comments in a Jira event, oldest first: the `comments`
/// list the app sends with the ticket, with a body and an id.
pub fn existing_jira_comments(payload: &Value) -> Vec<Existing> {
    let all: Vec<Existing> = payload["comments"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| {
            let id = match &c["id"] {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                _ => return None,
            };
            let text = common::jira_text::body_text(&c["body"]);
            if text.trim().is_empty() {
                return None;
            }
            Some(Existing {
                id,
                author: c["author"]["displayName"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                kind: c["author"]["accountType"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                text,
            })
        })
        .collect();
    let skip = all.len().saturating_sub(MAX_EXISTING);
    all.into_iter().skip(skip).collect()
}

/// A comment on a GitHub issue (not a pull request) whose ticket has a run
/// working on it: taken for that run. `false` when no run is working on
/// the issue in this repository, or the commenter has no standing.
pub async fn on_github_issue_comment(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> bool {
    if payload["action"].as_str() != Some("created") || payload["issue"]["pull_request"].is_object()
    {
        return false;
    }
    let number = payload["issue"]["number"].as_u64().unwrap_or(0);
    if number == 0 {
        return false;
    }
    let owner = payload["repository"]["owner"]["login"]
        .as_str()
        .unwrap_or("");
    let name = payload["repository"]["name"].as_str().unwrap_or("");
    let ticket_id = format!("GH-{number}");
    let Some(run) = working_run(state, team_id, &ticket_id).await else {
        return false;
    };
    if !run.repo.eq_ignore_ascii_case(&format!("{owner}/{name}")) {
        return false;
    }
    let user = &payload["comment"]["user"];
    let author = user["login"].as_str().unwrap_or("");
    let kind = user["type"].as_str().unwrap_or("");
    let text = payload["comment"]["body"].as_str().unwrap_or("");
    if !tc::is_person(Source::Github, author, kind, text) {
        return false;
    }
    // Only a person with standing in the repository or on the team may
    // direct a run, as for any comment that starts work.
    if !super::github_webhook::may_start_work(
        state,
        team_id,
        installation_id,
        payload,
        &payload["comment"],
        "comment",
    )
    .await
    {
        return false;
    }
    let id = payload["comment"]["id"]
        .as_u64()
        .map(|i| i.to_string())
        .unwrap_or_default();
    receive(
        state,
        &Incoming {
            team_id,
            installation_id,
            source: Source::Github,
            ticket_id: &ticket_id,
            comment_id: &id,
            author,
            author_kind: kind,
            text,
            title: payload["issue"]["title"].as_str().unwrap_or(""),
            run_id: &run.run_id,
            run_status: &run.status,
            pr_number: run.pr_number,
            repo_owner: owner,
            repo_name: name,
        },
    )
    .await
    .is_ok()
}

/// The ticket's run that is working now.
struct WorkingRun {
    run_id: String,
    status: String,
    repo: String,
    pr_number: u64,
}

async fn working_run(state: &AppState, team_id: &str, ticket_id: &str) -> Option<WorkingRun> {
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("ticket-index")
        .key_condition_expression("team_id = :tid AND ticket_id = :ticket")
        .expression_attribute_values(":tid", AttributeValue::S(team_id.to_string()))
        .expression_attribute_values(":ticket", AttributeValue::S(ticket_id.to_string()))
        .send()
        .await
        .map_err(|e| warn!(ticket_id, error = %e, "Could not look up the ticket's runs"))
        .ok()?;
    let text = |i: &std::collections::HashMap<String, AttributeValue>, k: &str| {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    out.items()
        .iter()
        .filter(|i| tc::run_working(&text(i, "status")))
        .max_by_key(|i| text(i, "run_id"))
        .map(|i| WorkingRun {
            run_id: text(i, "run_id"),
            status: text(i, "status"),
            repo: text(i, "repo"),
            pr_number: i
                .get("pr_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0),
        })
}

/// The reaction on a GitHub comment: seen, and being read.
async fn react(state: &AppState, inc: &Incoming<'_>) {
    if inc.comment_id.is_empty() || inc.repo_owner.is_empty() || inc.repo_name.is_empty() {
        return;
    }
    let token = match crate::auth::github_app::get_installation_token(state, inc.installation_id)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            warn!(ticket_id = inc.ticket_id, error = %e, "No GitHub token to acknowledge the comment");
            return;
        }
    };
    let url = format!(
        "https://api.github.com/repos/{}/{}/issues/comments/{}/reactions",
        inc.repo_owner, inc.repo_name, inc.comment_id
    );
    super::merge_control::gh_post(state, &token, &url, &json!({ "content": "eyes" })).await;
}

/// The one short reply on a Jira ticket, through the team's Jira app.
async fn reply(state: &AppState, inc: &Incoming<'_>) {
    let config = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", AttributeValue::S(inc.team_id.to_string()))
        .key("sk", AttributeValue::S("JIRA#config".into()))
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let Some(url) = config
        .as_ref()
        .and_then(|c| c.get("add_comment_url"))
        .and_then(|v| v.as_s().ok())
        .filter(|u| !u.is_empty())
    else {
        warn!(
            ticket_id = inc.ticket_id,
            "No Jira comment address to acknowledge the comment"
        );
        return;
    };
    let payload = json!({
        "issueKey": inc.ticket_id,
        "comment": tc::ack_reply(),
        "commentType": "info",
        "title": "Comment received",
    });
    match state.http.post(url).json(&payload).send().await {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => warn!(
            ticket_id = inc.ticket_id,
            status = r.status().as_u16(),
            "Jira refused the acknowledgement"
        ),
        Err(e) => warn!(ticket_id = inc.ticket_id, error = %e, "Jira acknowledgement failed"),
    }
}

/// Most progress notes a run record holds (the worker moves older ones off
/// the record; see its `run_lists`).
const MAX_RUN_NOTES: usize = 250;

/// Adds a note to the run page's activity.
async fn note(state: &AppState, team_id: &str, run_id: &str, message: &str) {
    let now = chrono::Utc::now().to_rfc3339();
    let entry = AttributeValue::M(std::collections::HashMap::from([
        (
            "message".to_string(),
            AttributeValue::S(common::truncate_str(message, 1000).to_string()),
        ),
        ("timestamp".to_string(), AttributeValue::S(now.clone())),
    ]));
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .update_expression(
            "SET progress_notes = list_append(if_not_exists(progress_notes, :empty), :n)",
        )
        .condition_expression(
            "attribute_exists(run_id) AND (attribute_not_exists(progress_notes) OR size(progress_notes) < :max)",
        )
        .expression_attribute_values(":empty", AttributeValue::L(vec![]))
        .expression_attribute_values(":n", AttributeValue::L(vec![entry]))
        .expression_attribute_values(":max", AttributeValue::N(MAX_RUN_NOTES.to_string()))
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not note the ticket comment on the run");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::ticket_comments::Memory;

    fn incoming<'a>(id: &'a str, author: &'a str, kind: &'a str, text: &'a str) -> Incoming<'a> {
        Incoming {
            team_id: "TEAM#1",
            installation_id: 7,
            source: Source::Jira,
            ticket_id: "ABC-1",
            comment_id: id,
            author,
            author_kind: kind,
            text,
            title: "Add the flag",
            run_id: "01RUN",
            run_status: "running",
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_comment_during_a_run_is_kept_and_acknowledged_once() {
        let store = Memory::default();
        let inc = incoming("10042", "Pat Doe", "atlassian", "Production only.");
        let Received::Kept(c, ack) = take(&store, &inc).await.unwrap() else {
            panic!("the comment was not kept")
        };
        assert_eq!(ack, Ack::Reply, "Jira: one short reply");
        assert_eq!(c.run_id, "01RUN");
        assert_eq!(c.text, "Production only.");
        // The webhook delivers it again (and again): one record, and no
        // second acknowledgement, note or message.
        for _ in 0..3 {
            assert_eq!(take(&store, &inc).await.unwrap(), Received::Duplicate);
        }
        assert_eq!(store.all("TEAM#1", "ABC-1").len(), 1);
    }

    #[tokio::test]
    async fn github_gets_a_reaction_and_every_working_status_counts() {
        let store = Memory::default();
        for (i, status) in ["running", "queued", "awaiting_ci"].into_iter().enumerate() {
            let id = format!("{i}");
            let inc = Incoming {
                source: Source::Github,
                ticket_id: "GH-12",
                run_status: status,
                ..incoming(&id, "pat-doe", "User", "Production only.")
            };
            let Received::Kept(_, ack) = take(&store, &inc).await.unwrap() else {
                panic!("not kept at {status}")
            };
            assert_eq!(ack, Ack::Reaction, "at {status}");
        }
    }

    #[tokio::test]
    async fn coderhelms_own_comments_and_bots_are_never_taken() {
        let store = Memory::default();
        let own_reply = tc::ack_reply();
        for (author, kind, text) in [
            ("CoderHelm", "app", "Questions only a person can answer"),
            (
                "Coderhelm",
                "",
                "I'm building the rest of the plan meanwhile",
            ),
            // The acknowledgement itself, whatever name the tracker shows.
            ("Some App", "", own_reply.as_str()),
            ("Automation for Jira", "app", "Moved to In Progress"),
        ] {
            let inc = incoming("1", author, kind, text);
            assert_eq!(take(&store, &inc).await.unwrap(), Received::NotAPerson);
        }
        let gh = Incoming {
            source: Source::Github,
            ..incoming("2", "coderhelm[bot]", "Bot", "Working on it")
        };
        assert_eq!(take(&store, &gh).await.unwrap(), Received::NotAPerson);
        assert!(
            store.all("TEAM#1", "ABC-1").is_empty(),
            "nothing kept, so nothing can come back"
        );
    }

    #[tokio::test]
    async fn a_comment_with_no_run_working_is_kept_without_an_acknowledgement() {
        let store = Memory::default();
        for status in [
            "",
            "completed",
            "failed",
            "needs_input",
            "cancelled",
            "closed",
        ] {
            let id = format!("c-{status}");
            let inc = Incoming {
                run_status: status,
                ..incoming(&id, "Pat Doe", "", "Context for whoever picks this up.")
            };
            let Received::Kept(c, ack) = take(&store, &inc).await.unwrap() else {
                panic!("not kept at {status:?}")
            };
            assert_eq!(ack, Ack::None, "at {status:?}");
            assert!(!c.during_run(), "context, never a stop: {status:?}");
        }
    }

    #[test]
    fn the_tickets_existing_comments_are_read_from_the_event() {
        let payload = json!({"comments": [
            {"id": "1", "author": {"displayName": "Pat Doe", "accountType": "atlassian"},
             "body": "Production only."},
            {"id": 2, "author": {"displayName": "CoderHelm", "accountType": "app"},
             "body": "Questions only a person can answer"},
            {"id": "3", "author": {"displayName": "Sam Roe"}, "body": "  "},
            {"author": {"displayName": "No Id"}, "body": "x"}
        ]});
        let got = existing_jira_comments(&payload);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].id, "1");
        assert_eq!(got[0].text, "Production only.");
        assert_eq!(got[1].kind, "app");
        // An event without the list (an older app) reads as none.
        assert!(existing_jira_comments(&json!({"issue": {}})).is_empty());
        // Only the newest are read.
        let many: Vec<Value> = (0..50)
            .map(|i| json!({"id": i.to_string(), "author": {"displayName": "Pat"}, "body": "x"}))
            .collect();
        let got = existing_jira_comments(&json!({ "comments": many }));
        assert_eq!(got.len(), MAX_EXISTING);
        assert_eq!(got.last().unwrap().id, "49");
    }

    #[tokio::test]
    async fn a_ticket_whose_comments_were_never_stored_gets_them_from_the_event_that_starts_the_run(
    ) {
        // People wrote on a ticket CoderHelm had nothing to do with: none
        // of it was stored. The ticket is then assigned to CoderHelm, and
        // the event that starts the run carries the comments.
        let store = Memory::default();
        assert!(store.all("TEAM#1", "ABC-1").is_empty());
        let event = json!({"comments": [
            {"id": "1", "author": {"displayName": "Pat Doe", "accountType": "atlassian"},
             "body": "Production only."},
            {"id": "2", "author": {"displayName": "Sam Roe", "accountType": "atlassian"},
             "body": "Use the existing export helper."},
            {"id": "3", "author": {"displayName": "CoderHelm", "accountType": "app"},
             "body": "An earlier note of its own"}
        ]});
        assert!(event_carries_comments(&event));
        assert_eq!(keep_existing_in(&store, "TEAM#1", "ABC-1", &event).await, 2);
        // Kept as context for the run: open, never a stop, no run named.
        let open = tc::open(store.all("TEAM#1", "ABC-1"));
        assert_eq!(open.len(), 2);
        assert!(open.iter().all(|c| !c.during_run()));
        // The run reads them at its first boundary, while it plans.
        let s = tc::settle_open(&store, "TEAM#1", "01RUN", &open, tc::Stage::Planning, None).await;
        assert_eq!(s.with(tc::Action::Fold).len(), 2);
        // The same comments with a later event are not taken again.
        assert_eq!(keep_existing_in(&store, "TEAM#1", "ABC-1", &event).await, 0);
        assert!(tc::open(store.all("TEAM#1", "ABC-1")).is_empty());
    }

    #[test]
    fn an_event_from_an_older_jira_app_is_told_apart_from_a_ticket_without_comments() {
        // The app sends the list, empty for a ticket nobody commented on.
        assert!(event_carries_comments(
            &json!({"issue": {}, "comments": []})
        ));
        // An older app (or a plain Jira webhook) sends no list at all.
        assert!(!event_carries_comments(&json!({"issue": {}})));
        assert!(!event_carries_comments(&json!({"comments": null})));
    }

    #[test]
    fn a_comment_before_a_run_is_stored_only_for_a_ticket_coderhelm_works() {
        let jira = include_str!("jira_webhook.rs");
        let branch = &jira[jira
            .find("let Some(run_item) = run_item_found.as_ref() else {")
            .unwrap()..];
        let branch = &branch[..branch.find("let run_status = run_item").unwrap()];
        // The store is reached only through the trigger check.
        let gate = branch
            .find("stores_before_run(false, has_label, is_assigned)")
            .unwrap();
        assert!(gate < branch.find("super::ticket_comments::keep(").unwrap());
        // The same trigger rule that starts a run.
        assert!(
            jira.matches("ticket_triggers(issue, &trigger_label)")
                .count()
                >= 2
        );
    }

    #[test]
    fn the_webhooks_no_longer_drop_a_comment_for_a_run_in_progress() {
        let jira = include_str!("jira_webhook.rs");
        assert!(!jira.contains("Ignoring Jira comment"));
        assert!(!jira.contains("\"comment_skipped\""));
        assert!(jira.contains("super::ticket_comments::receive("));
        assert!(jira.contains("super::ticket_comments::keep("));
        let github = include_str!("github_webhook.rs");
        assert!(github.contains("super::ticket_comments::on_github_issue_comment("));
    }
}
