//! cb. A person wrote "@CoderHelm rebase with main" on a pull request.
//! CoderHelm merged main in (force pushes were off) and replied with a
//! quote of that very comment, an "@name, replying to your comment" header
//! linking it, the person @-mentioned a second time, "you asked for a
//! rebase with main:", two clauses on how the merge was done, and "It's
//! pushed to pull request #N" linking the pull request the reply sat on.
//! The owner objected to how long and noisy it was.
//!
//! Guarantee: the branch update says its outcome in one line ("Merged main
//! in (7c0ffee).", plus one short reason only when it is a merge where a
//! rebase was asked), posted as written with no model rewriting it. Every
//! comment passes the thread rules (`comment_style::tidy` and `lint_on`):
//! under the newest comment a reply carries no quote, no header and no
//! name; under an older one, a quote only; one @-mention of a person; never
//! a link to the thread it is on; never the request said back.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use common::comment_style::{lint_on, tidy};
use common::conversation::{self as cv, Channel, Heard, How, Kind, Meta, Place, Said, Say, View};
use common::reply_compose::{Compose, Composer, Facts};
use serde_json::{json, Value};

use crate::passes::branch_update::{asks_rebase, reply, MergeWhy, Outcome};

const T0: i64 = 1_900_000_000_000;
const SHA: &str = "7c0ffee3a9d1b5";

/// What the model wrote (names changed).
const LONG_REPLY: &str = "@sam you asked for a rebase with main: the branch is now up to date with main as a merge commit (7c0ffee), because force pushes aren't allowed here, so the result is the same code as a rebase. It's pushed to [pull request #482](https://github.com/acme/web/pull/482).";

fn place() -> Place {
    Place::github("acme", "web", 482)
}

/// The thread: CoderHelm's status from earlier, then sam's comment.
fn thread(also_after: bool) -> View {
    let mut heard = vec![Heard {
        id: "900".into(),
        author: "sam".into(),
        text: "@CoderHelm rebase with main".into(),
        at_ms: T0 + 60_000,
        url: "https://github.com/acme/web/pull/482#issuecomment-900".into(),
        ..Default::default()
    }];
    if also_after {
        heard.push(Heard {
            id: "901".into(),
            author: "kim".into(),
            text: "I'll look at the copy after that.".into(),
            at_ms: T0 + 120_000,
            ..Default::default()
        });
    }
    View {
        said: vec![Said {
            id: "800".into(),
            at_ms: T0,
            meta: Meta {
                slot: "run".into(),
                kind: Kind::Status,
                fp: cv::fingerprint("Opened for review."),
                gist: "Opened for review.".into(),
                first_ms: T0,
                ..Default::default()
            },
            body: "Opened for review.".into(),
            node_id: String::new(),
        }],
        heard,
    }
}

struct Thread {
    view: Mutex<View>,
    log: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl Channel for Thread {
    fn place(&self) -> Place {
        place()
    }
    async fn read(&self) -> Result<View, String> {
        Ok(self.view.lock().unwrap().clone())
    }
    async fn create(&self, body: &str, _: &Meta) -> Result<String, String> {
        self.log.lock().unwrap().push(format!("create: {body}"));
        Ok("c1".into())
    }
    async fn edit(&self, id: &str, body: &str, _: &Meta) -> Result<String, String> {
        self.log.lock().unwrap().push(format!("edit {id}: {body}"));
        Ok(id.into())
    }
    async fn reply(&self, to: &Heard, _: How, body: &str, _: &Meta) -> Result<String, String> {
        self.log
            .lock()
            .unwrap()
            .push(format!("reply {}: {body}", to.id));
        Ok("r1".into())
    }
    async fn react(&self, _: &Heard, _: &str) -> Result<(), String> {
        Ok(())
    }
}

/// The model, answering what it answered in the incident.
struct Recorded(AtomicUsize);

#[async_trait::async_trait]
impl Composer for Recorded {
    async fn compose(&self, _: &str, _: &str, _: &Value) -> Option<Value> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Some(json!({"action": "reply", "text": LONG_REPLY}))
    }
}

fn facts() -> Facts {
    Facts::default()
        .github_pr("acme/web", 482, "Slip map page", "open")
        .github_commit("acme/web", SHA, "Merge main into coderhelm/abc-1")
        .state("the branch update is pushed")
}

/// Replays the reply on the thread; what was written to it.
async fn replay(view: View, line: &str, facts: &Facts, model: &Recorded) -> Vec<String> {
    let ch = Thread {
        view: Mutex::new(view),
        log: Mutex::new(Vec::new()),
    };
    let c = Compose {
        composer: model,
        facts,
    };
    cv::speak_with(
        &ch,
        None,
        Some(c),
        // `Say::reply_all("feedback", line)`, as the feedback pass says it.
        &Say {
            slot: "feedback".into(),
            kind: Kind::Reply,
            body: line.into(),
            answers: vec![cv::ANSWERS_ALL.into()],
            ..Default::default()
        },
        T0 + 600_000,
    )
    .await
    .unwrap();
    ch.log.into_inner().unwrap()
}

#[test]
fn the_branch_update_says_its_outcome_in_one_line() {
    let merged = Outcome::Merged {
        sha: SHA.into(),
        why: MergeWhy::ForcePushNotAllowed,
    };
    // They asked for a rebase and got a merge: one short reason.
    assert_eq!(
        reply(&merged, "main", asks_rebase("rebase with main"), false),
        "Merged main in (7c0ffee). Force pushes are off here, so it's a merge instead of a rebase."
    );
    // They asked to merge main in: nothing to explain.
    assert_eq!(
        reply(&merged, "main", asks_rebase("merge main in"), false),
        "Merged main in (7c0ffee)."
    );
}

#[tokio::test]
async fn the_reply_under_the_newest_comment_is_the_line_and_nothing_else() {
    let line = reply(
        &Outcome::Merged {
            sha: SHA.into(),
            why: MergeWhy::ForcePushNotAllowed,
        },
        "main",
        true,
        false,
    );
    let model = Recorded(AtomicUsize::new(0));
    let log = replay(thread(false), &line, &facts().as_written(), &model).await;
    assert_eq!(
        log,
        vec![format!(
            "reply 900: Merged main in (7c0ffee). Force pushes are off here, so it's a merge instead of a rebase."
        )]
    );
    // The line is the answer: no model rewrote it at length.
    assert_eq!(model.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn whatever_a_model_writes_the_thread_rules_hold() {
    // Another path still asks the model, and it answers as it did in the incident.
    let model = Recorded(AtomicUsize::new(0));
    let mut facts = facts();
    facts.state = "the branch update is pushed".into();
    let log = replay(thread(false), "Merged main in (7c0ffee).", &facts, &model).await;
    assert_eq!(model.0.load(Ordering::SeqCst), 1);
    assert_eq!(log.len(), 1, "{log:?}");
    let body = log[0].strip_prefix("reply 900: ").expect("a reply to sam");
    for never in ["> ", "replying to", "@sam", "you asked", "pull/482", "#482"] {
        assert!(!body.contains(never), "{never}: {body}");
    }
    assert!(body.contains("7c0ffee"), "{body}");
    assert!(lint_on(body, &place()).is_empty(), "{body}");
}

#[tokio::test]
async fn under_an_older_comment_the_reply_quotes_it_and_nothing_more() {
    let model = Recorded(AtomicUsize::new(0));
    let log = replay(
        thread(true),
        "Merged main in (7c0ffee).",
        &facts().as_written(),
        &model,
    )
    .await;
    // kim wrote after sam: the reply answers the newest comment (kim's),
    // or quotes the one it answers; never a header, never a name.
    assert_eq!(log.len(), 1, "{log:?}");
    let body = log[0].split_once(": ").unwrap().1;
    assert!(
        !body.contains("replying to") && !body.contains('@'),
        "{body}"
    );
    assert!(body.ends_with("Merged main in (7c0ffee)."), "{body}");
    assert!(lint_on(body, &place()).is_empty(), "{body}");
}

#[test]
fn the_old_reply_breaks_the_rules_and_tidy_fixes_what_it_can() {
    let old = format!(
        "> @CoderHelm rebase with main\n\n@sam, replying to [your comment](https://github.com/acme/web/pull/482#issuecomment-900).\n\n{LONG_REPLY}"
    );
    assert!(!lint_on(&old, &place()).is_empty());
    let after = tidy(&old, &place());
    assert!(
        !after.contains("replying to") && !after.contains("@sam") && !after.contains("pull/482"),
        "{after}"
    );
    assert!(!after.to_lowercase().contains("you asked"), "{after}");
    assert!(lint_on(&after, &place()).is_empty(), "{after}");
}
