//! aq. A run started from a ticket asked three questions on it ("Questions
//! only a person can answer"), proposed its defaults and kept building.
//! Twenty minutes later a person answered on the ticket. The gateway logged
//! "Ignoring Jira comment — run already in progress" and dropped it: nothing
//! acknowledged the comment, the run never saw it, and its notes kept saying
//! "3 question(s) already asked on the ticket — not asked again; the run goes
//! on without the answers". Comments written on the ticket before any run
//! existed were dropped the same way ("No runs found for Jira comment —
//! ignoring"), and nothing read them when the run started.
//!
//! Guarantee: a person who writes on a ticket CoderHelm is working on gets
//! an acknowledgement, and what they wrote is acted on. The comment is kept
//! once per comment id and acknowledged once (a reaction on GitHub, one
//! short reply in Jira; never for CoderHelm's own comments). The run reads
//! its open comments at its next step boundary, together: an answer or an
//! instruction goes into the plan while it is being made; once the plan is
//! final it becomes one round on the pull request when the step ends, where
//! the code changes in the place the plan put that choice, and the ticket
//! is told; a comment that says to stop ends the run at once. A comment
//! nobody read yet stays open for whatever comes next, and comments written
//! before the run started are read at its first boundary.

use super::fixture::*;
use crate::passes::ticket_comments::{self as worker_side, effects};
use common::ticket_comments::{
    self as tc, Ack, Action, Class, Comment, Memory, Source, Stage, State, Store,
};
use serde_json::json;

/// What the gateway used to log, and what the run's notes used to say.
const OLD_DROP: &str = "run already in progress";
const OLD_NOTE: &str = "goes on without the answers";
const OLD_IGNORED: &str = "ignoring";

const TEAM: &str = "TEAM#1";
const TICKET: &str = "ABC-1";
const RUN: &str = "01RUN";

fn questions() -> Vec<String> {
    vec![
        "Which environments get the new flag: production only, or staging too?".to_string(),
        "Keep the old column for one release, or drop it now?".to_string(),
        "Who may see the export: admins only, or every member?".to_string(),
    ]
}

/// The comment as the tracker delivers it while the run is building.
fn answer_delivery() -> (String, String, String, String) {
    (
        "10042".to_string(),
        "Pat Doe".to_string(),
        "atlassian".to_string(),
        "Production only. Keep the old column for one release. Admins only.".to_string(),
    )
}

/// The gateway's part, as `routes::ticket_comments::take` does it: a
/// person's comment is kept once, with the run that was working.
async fn deliver(
    store: &Memory,
    (id, author, kind, text): &(String, String, String, String),
    run_status: &str,
) -> Option<(Comment, Ack)> {
    if !tc::is_person(Source::Jira, author, kind, text) {
        return None;
    }
    let working = tc::run_working(run_status);
    let c = Comment {
        ticket_id: TICKET.into(),
        source: Source::Jira,
        comment_id: tc::comment_id(id, author, text),
        author: author.clone(),
        text: text.clone(),
        at: chrono::Utc::now().to_rfc3339(),
        run_id: if working { RUN.into() } else { String::new() },
        ..Default::default()
    };
    let new = store.record(TEAM, &c).await.unwrap();
    new.then(|| (c, tc::ack_for(Source::Jira, true, new, working)))
}

/// One read of the ticket's open comments at `stage`, with the model's
/// reply for the unread ones.
async fn read_at(
    store: &Memory,
    stage: Stage,
    model_reply: &serde_json::Value,
    pr: Option<&str>,
    end: &mut Ending,
) -> worker_side::Effects {
    let mut open = tc::open(store.all(TEAM, TICKET));
    let unread = open.iter().filter(|c| c.class.is_none()).count();
    if unread > 0 {
        let readings = tc::readings(model_reply, &questions(), unread);
        tc::keep_readings(store, TEAM, &mut open, &readings).await;
    }
    let settled = tc::settle_open(store, TEAM, RUN, &open, stage, pr).await;
    let fx = effects(&settled, pr);
    for n in &fx.run_notes {
        end.said(n.clone());
    }
    if let Some(r) = &fx.reply {
        end.said(r.clone());
    }
    fx
}

#[tokio::test]
async fn an_answer_written_while_the_run_builds_is_acknowledged_read_and_applied() {
    let store = Memory::default();
    let mut end = Ending::default();

    // Twenty minutes in, the run is building from its final plan.
    let (c, ack) = deliver(&store, &answer_delivery(), "running")
        .await
        .expect("the comment is kept, never dropped");
    assert_eq!(ack, Ack::Reply, "one short reply on the ticket");
    end.said(tc::ack_reply());
    end.said(tc::received_note(&c, ack));
    assert!(end.says("Pat Doe commented on the ticket"));

    // The webhook is delivered again: no second record or acknowledgement.
    assert!(deliver(&store, &answer_delivery(), "running")
        .await
        .is_none());
    // CoderHelm's own reply comes back as a comment event: never read.
    let own = (
        "10043".to_string(),
        "Coderhelm".to_string(),
        "app".to_string(),
        tc::ack_reply(),
    );
    assert!(deliver(&store, &own, "running").await.is_none());
    assert_eq!(store.all(TEAM, TICKET).len(), 1);

    // Read as soon as it arrives (outside the run's step): it answers the
    // run's questions and doesn't say to stop, so it waits for the run.
    let model = json!({"comments": [
        {"n": 1, "class": "answer", "question": 1, "reason": "answers the three questions"}
    ]});
    let fx = read_at(&store, Stage::BetweenSteps, &model, None, &mut end).await;
    assert_eq!(fx, worker_side::Effects::default());
    assert_eq!(
        tc::open(store.all(TEAM, TICKET))[0].class,
        Some(Class::Answer)
    );

    // The run's next step boundary: the plan is final, so the answer is
    // kept for the pull request, and the run page says so.
    let fx = read_at(&store, Stage::Building, &json!({}), None, &mut end).await;
    assert!(
        fx.folded.is_empty() && fx.round.is_none(),
        "nothing yet but the note"
    );
    assert!(end.says("Read Pat Doe's comment as an answer to \"Which environments"));
    assert!(end.says("applied as a round on the pull request when this step ends"));
    assert_eq!(tc::open(store.all(TEAM, TICKET))[0].state, State::Deferred);

    // The step ends with the pull request open: one round on it, with the
    // answer and where to apply it, and the ticket is told.
    let pr = "https://example.test/acme/web/pull/12";
    let done = Stage::Finished {
        pr_open: true,
        waiting: false,
    };
    let fx = read_at(&store, done, &json!({}), Some(pr), &mut end).await;
    let round = fx.round.expect("a feedback round on the open pull request");
    assert!(round.contains("Answer: Production only. Keep the old column"));
    assert!(round.contains(
        "Where this answer differs from the default that was built, change the code in the place the plan put that choice"
    ));
    assert!(round.contains("Say in the pull request"));
    assert!(end.says("I'm now changing the code where that choice lives to match the answer"));
    assert!(end.says(pr));
    end.status = "completed".into();
    end.pr = Some(12);

    // Acted on once: nothing is left, whoever reads next.
    assert!(tc::open(store.all(TEAM, TICKET)).is_empty());
    let fx = read_at(&store, done, &json!({}), Some(pr), &mut end).await;
    assert_eq!(fx, worker_side::Effects::default());

    end.never_says(OLD_DROP);
    end.never_says(OLD_NOTE);
    end.never_says(OLD_IGNORED);
}

#[tokio::test]
async fn comments_written_before_the_run_are_read_when_it_starts() {
    let store = Memory::default();
    let mut end = Ending::default();
    // No run yet: kept without an acknowledgement, as context.
    let early = (
        "10001".to_string(),
        "Sam Roe".to_string(),
        "atlassian".to_string(),
        "Use the existing export helper, don't add a new one.".to_string(),
    );
    let (c, ack) = deliver(&store, &early, "").await.expect("kept");
    assert_eq!(ack, Ack::None);
    assert!(!c.during_run());

    // The run starts and reads the ticket at its first boundary, while it
    // plans: the planner gets the comment word for word.
    let model = json!({"comments": [
        {"n": 1, "class": "instruction", "question": 0, "reason": "says how to build it"}
    ]});
    let fx = read_at(&store, Stage::Planning, &model, None, &mut end).await;
    assert_eq!(fx.folded.len(), 1);
    let note = common::run_questions::answers_note(&fx.folded);
    assert!(note.contains("Answer from Sam Roe: Use the existing export helper"));
    assert!(end.says("folded into the plan before any code was written"));
    assert_eq!(fx.reply, None, "nothing more is posted on the ticket");
    // Even one that says to stop is context: it was written before the run.
    assert_eq!(
        tc::decide(Class::Stop, Stage::Planning, false, false),
        Action::Fold
    );
    end.never_says(OLD_IGNORED);
}

#[tokio::test]
async fn whatever_state_the_run_is_in_the_comment_is_not_lost() {
    let model = json!({"comments": [
        {"n": 1, "class": "answer", "question": 1, "reason": ""}
    ]});
    let pr_open = Stage::Finished {
        pr_open: true,
        waiting: false,
    };
    let waiting = Stage::Finished {
        pr_open: false,
        waiting: true,
    };
    // (the stage when the comment is read, what becomes of it)
    for (stage, expect) in [
        (Stage::Planning, Action::Fold),
        (Stage::Building, Action::Defer),
        (Stage::Reviewing, Action::Defer),
        (pr_open, Action::Round),
        (waiting, Action::Restart),
        (Stage::Cancelled, Action::Keep),
    ] {
        let store = Memory::default();
        let mut end = Ending::default();
        deliver(&store, &answer_delivery(), "running")
            .await
            .unwrap();
        // Handed off between steps first: it waits, still open.
        read_at(&store, Stage::BetweenSteps, &model, None, &mut end).await;
        assert_eq!(tc::open(store.all(TEAM, TICKET)).len(), 1, "{stage:?}");
        let fx = read_at(&store, stage, &json!({}), Some("PR"), &mut end).await;
        match expect {
            Action::Fold => assert_eq!(fx.folded.len(), 1),
            Action::Defer => {
                assert_eq!(tc::open(store.all(TEAM, TICKET))[0].state, State::Deferred)
            }
            Action::Round => assert!(fx.round.is_some()),
            Action::Restart => assert!(fx.restart.is_some()),
            Action::Keep => assert_eq!(fx.ticket_notes.len(), 1),
            other => panic!("unexpected {other:?}"),
        }
        assert!(
            !end.words.is_empty(),
            "the run page says what was done at {stage:?}"
        );
        end.never_says(OLD_NOTE);
    }
}

#[test]
fn the_pipeline_and_the_webhooks_take_the_comment() {
    // The run reads its ticket at its boundaries, and when a step ends.
    let run = run_passes_src();
    assert!(run.contains("read_ticket_comments(state, msg, run_id, usage)"));
    let src = include_str!("../mod.rs");
    assert!(src.contains("ticket_comments::after_step(state, &msg, &run_id)"));
    // The note that said the run goes on without the answers is gone.
    assert!(!src.contains("the run goes on without the answers"));
    // The gateway keeps the comment where it used to drop it.
    let jira = include_str!("../../../../gateway/src/routes/jira_webhook.rs");
    assert!(!jira.contains("Ignoring Jira comment"));
    assert!(!jira.contains("No runs found for Jira comment — ignoring"));
    assert!(jira.contains("super::ticket_comments::receive("));
    let github = include_str!("../../../../gateway/src/routes/github_webhook.rs");
    assert!(!github.contains("Skipping slash command — run already in flight"));
    assert!(github.contains("super::ticket_comments::on_github_issue_comment("));
}
