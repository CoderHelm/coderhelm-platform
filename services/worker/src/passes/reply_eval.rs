//! The quality eval for what CoderHelm says back to people
//! (`common::reply_compose`).
//!
//! Each case is a realistic thread with every name made up: CoderHelm's own
//! earlier comments, the person's comment it answers, the path's draft (what
//! that path posted before the composer), the run's records, and the model's
//! answer, recorded so the eval is deterministic and makes no call. A case
//! runs the same function production runs (`conversation::speak_with`, or
//! `reply_compose::apply` for a review thread) against an in-memory thread
//! and is graded:
//!
//! - what was posted: a reply, a reaction, or nothing;
//! - what the reply must say (the commit, the reason, the answer) and must
//!   not (a generic line, a claim nothing records, a question back);
//! - the general rubric on every reply: no generic line, no hand-back, no
//!   question the run didn't need, no arrow glyph, short, and every claim in
//!   it borne out by the records (`reply_compose::check` takes nothing out
//!   of it again).
//!
//! Some cases feed a bad model answer on purpose (an unrecorded commit, a
//! question bounced back, "Thanks for your comment"): the guards must
//! reduce it or fall back to the draft.
//!
//! The same threads run against the live model with
//! `ANTHROPIC_API_KEY=... cargo test -p worker reply_eval -- --ignored`
//! ([`live_model_writes_the_replies`]); without a key it is skipped.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use common::conversation::{
    self as cv, Channel, Decision, Heard, How, Kind, Meta, Place, Said, Say, Spoke, View,
};
use common::reply_compose::{self as rc, Compose, Composer, Facts};
use serde_json::{json, Value};

const T0: i64 = 1_795_000_000_000;

/// A thread in memory: what was written on it, in order.
struct Thread {
    place: Place,
    view: Mutex<View>,
    log: Mutex<Vec<String>>,
}

impl Thread {
    fn new(place: Place, view: View) -> Thread {
        Thread {
            place,
            view: Mutex::new(view),
            log: Mutex::new(Vec::new()),
        }
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    fn written(&self) -> Vec<String> {
        self.log()
            .into_iter()
            .filter(|l| !l.starts_with("react "))
            .collect()
    }
}

#[async_trait::async_trait]
impl Channel for Thread {
    fn place(&self) -> Place {
        self.place.clone()
    }
    async fn read(&self) -> Result<View, String> {
        Ok(self.view.lock().unwrap().clone())
    }
    async fn create(&self, body: &str, meta: &Meta) -> Result<String, String> {
        let mut v = self.view.lock().unwrap();
        let id = format!("n{}", v.said.len());
        v.remember(&id, T0 + 9_000_000, meta);
        self.log
            .lock()
            .unwrap()
            .push(format!("create {id}: {body}"));
        Ok(id)
    }
    async fn edit(&self, id: &str, body: &str, meta: &Meta) -> Result<String, String> {
        let mut v = self.view.lock().unwrap();
        v.remember(id, T0 + 9_500_000, meta);
        if let Some(s) = v.said.iter_mut().find(|s| s.id == id) {
            s.body = body.to_string();
        }
        self.log.lock().unwrap().push(format!("edit {id}: {body}"));
        Ok(id.to_string())
    }
    async fn reply(&self, to: &Heard, how: How, body: &str, meta: &Meta) -> Result<String, String> {
        let mut v = self.view.lock().unwrap();
        let id = format!("r{}", v.said.len());
        v.remember(&id, T0 + 8_000_000, meta);
        self.log
            .lock()
            .unwrap()
            .push(format!("reply {} {how:?}: {body}", to.id));
        Ok(id)
    }
    async fn react(&self, to: &Heard, content: &str) -> Result<(), String> {
        if !self.place.markdown() {
            return Err("no reactions here".into());
        }
        self.log
            .lock()
            .unwrap()
            .push(format!("react {} {content}", to.id));
        Ok(())
    }
}

/// The model's recorded answer, and how often it was asked.
struct Recorded {
    answer: Value,
    calls: AtomicUsize,
}

impl Recorded {
    fn new(answer: Value) -> Recorded {
        Recorded {
            answer,
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl Composer for Recorded {
    async fn compose(&self, _: &str, _: &str, _: &Value) -> Option<Value> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Some(self.answer.clone())
    }
}

/// What a case expects was posted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Out {
    /// A reply to the person (on GitHub's conversation, Jira, or a thread).
    Reply,
    /// A reaction only, nothing written.
    React,
}

struct Case {
    name: &'static str,
    place: Place,
    view: View,
    say: Say,
    facts: Facts,
    /// The model's recorded answer.
    model: Value,
    out: Out,
    /// What the reply must say.
    has: Vec<&'static str>,
    /// What it must not say.
    not: Vec<&'static str>,
}

fn said(id: &str, slot: &str, body: &str, asks: &[&str], at_ms: i64) -> Said {
    Said {
        id: id.into(),
        at_ms,
        meta: Meta {
            slot: slot.into(),
            kind: Kind::Status,
            fp: cv::fingerprint(body),
            gist: cv::cut_words(&cv::visible(body), 160),
            asks: asks.iter().map(|a| a.to_string()).collect(),
            first_ms: at_ms,
            ..Default::default()
        },
        body: body.into(),
        node_id: String::new(),
    }
}

fn heard(id: &str, who: &str, text: &str, at_ms: i64) -> Heard {
    Heard {
        id: id.into(),
        author: who.into(),
        text: text.into(),
        at_ms,
        url: format!("https://github.com/northwind/storefront/issues/52#issuecomment-{id}"),
        ..Default::default()
    }
}

/// The same thread under another name: each test reads its own entries
/// of the composer's process-wide cache.
fn under(place: &Place, tag: &str) -> Place {
    match place {
        Place::Github { repo, number } => {
            let (o, r) = repo.split_once('/').unwrap_or(("", repo));
            Place::github(o, &format!("{r}-{tag}"), *number)
        }
        Place::Jira { ticket } => Place::jira(&format!("{ticket}{tag}")),
    }
}

fn storefront() -> Place {
    Place::github("northwind", "storefront", 52)
}

const H1_QUESTION: &str =
    "Should the catalog page keep the H1 and the intro copy from the design, or start at the page title?";

fn cases() -> Vec<Case> {
    vec![
        // The real shape: a teammate answers CoderHelm's question under it,
        // the round applies it and pushes.
        Case {
            name: "an instruction under CoderHelm's question is applied and confirmed with the commit",
            place: storefront(),
            view: View {
                said: vec![said(
                    "100",
                    "questions",
                    &format!("{H1_QUESTION}\n\nI'm building with the proposed default meanwhile; reply to this comment if you want something else."),
                    &[H1_QUESTION],
                    T0,
                )],
                heard: vec![heard(
                    "201",
                    "dana-reyes",
                    "Skip the H1 and intro copy. Just use the title Spring Catalog",
                    T0 + 60_000,
                )],
            },
            say: Say::reply_all(
                "feedback",
                "Removed the H1 and the intro block from the catalog page; it now opens on the title.",
            ),
            facts: Facts::default()
                .ticket("SHOP-210 Catalog page header", "Build the spring catalog page.")
                .github_pr("northwind/storefront", 52, "Spring catalog page", "open")
                .github_commit("northwind/storefront", "4f9c2a1d7e03b6", "Catalog page opens on its title")
                .file("src/pages/catalog.tsx")
                .check("Full test suite", "passed")
                .state("this round's change is pushed to the pull request"),
            model: json!({"action": "reply", "text": "Done: no H1 and no intro copy, the catalog page now opens on the title \"Spring Catalog\". The change is in `src/pages/catalog.tsx`, commit 4f9c2a1, and the full test suite passed."}),
            out: Out::Reply,
            has: vec!["4f9c2a1", "Spring Catalog", "catalog.tsx"],
            not: vec!["Thanks", "I read your comment", "?"],
        },
        // The real shape: someone retries a ticket an open pull request
        // already covers.
        Case {
            name: "a retry of a ticket an open pull request covers is answered with that pull request",
            place: Place::jira("SHOP-214"),
            view: View {
                said: vec![said(
                    "c1",
                    "run",
                    "Already being fixed in #41 (https://github.com/northwind/storefront/pull/41). CoderHelm didn't build a second fix.",
                    &[],
                    T0,
                )],
                heard: vec![Heard {
                    url: String::new(),
                    ..heard("j9", "Lee Park", "Retrying this one, please pick it up again.", T0 + 3_600_000)
                }],
            },
            say: Say::status(
                "run",
                "This ticket's change is in #41 (https://github.com/northwind/storefront/pull/41), CoderHelm's open pull request that already covered it: CoderHelm carried its work over there instead of opening a second pull request. Still open: the cart total test.",
            ),
            facts: Facts::default()
                .ticket("SHOP-214 Cart total is off by a cent", "Cart totals should match the invoice.")
                .pr(41, "https://github.com/northwind/storefront/pull/41", "Round cart lines to cents", "open")
                .state("this run ended without building anything: open pull request #41 already covers the ticket (continued)")
                .decision("Still open in #41: the cart total test"),
            model: json!({"action": "reply", "text": "#41 is still open and already carries this ticket's change, so a retry would only build the same fix twice; I didn't start one. What's left there is the cart total test, and the ticket is done when #41 merges."}),
            out: Out::Reply,
            has: vec!["#41", "cart total test"],
            not: vec!["?", "Thanks"],
        },
        Case {
            name: "why did it stop gets the recorded reason",
            place: Place::jira("SHOP-230"),
            view: View {
                said: vec![said(
                    "c4",
                    "run",
                    "GitHub's hourly request limit ran out. Nothing is needed from you: the run continues by itself once GitHub's hourly request limit resets.",
                    &[],
                    T0,
                )],
                heard: vec![Heard {
                    url: String::new(),
                    ..heard("j12", "Ana Ruiz", "why did it stop?", T0 + 120_000)
                }],
            },
            say: Say::reply_all(
                "ticket-comments",
                "Ana Ruiz's question (\"why did it stop?\"): where the work is now is below.\n\nWhere the work is: failed. Why it stopped: GitHub's hourly request limit for this installation ran out at 14:05 UTC; the run continues by itself when it resets at 15:00 UTC.",
            ),
            facts: Facts::default()
                .ticket("SHOP-230 Wishlist sync", "")
                .state("failed")
                .stopped("GitHub's hourly request limit for this installation ran out at 14:05 UTC; the run continues by itself when it resets at 15:00 UTC")
                .decision("Read Ana Ruiz's comment as a question to CoderHelm: answered on the ticket from the run's record."),
            model: json!({"action": "reply", "text": "It stopped because GitHub's hourly request limit for this installation ran out at 14:05 UTC. Nothing is needed from you: it picks up again by itself when the limit resets at 15:00 UTC."}),
            out: Out::Reply,
            has: vec!["hourly request limit", "15:00 UTC"],
            not: vec!["Add what to change", "?"],
        },
        Case {
            name: "a reply that claims an unrecorded commit, file and check falls back to the draft",
            place: storefront(),
            view: View {
                said: vec![],
                heard: vec![heard("301", "sam-okafor", "did CI pass on this?", T0)],
            },
            say: Say::reply_all("feedback", "The checks on #52 are still running; I post the result here when they finish."),
            facts: Facts::default()
                .github_pr("northwind/storefront", 52, "Spring catalog page", "open")
                .state("waiting for the pull request's checks"),
            model: json!({"action": "reply", "text": "Yes, all checks passed in commit deadbee1. I also updated `src/app.ts` to match."}),
            out: Out::Reply,
            has: vec!["still running"],
            not: vec!["deadbee1", "src/app.ts", "all checks passed"],
        },
        Case {
            name: "a question the records answer is never sent back",
            place: storefront(),
            view: View {
                said: vec![],
                heard: vec![heard("302", "priya-n", "which endpoint did you change for this?", T0)],
            },
            say: Say::reply_all("feedback", "The orders endpoint: `src/api/orders.ts` now rounds each line to cents."),
            facts: Facts::default()
                .github_pr("northwind/storefront", 52, "", "open")
                .github_commit("northwind/storefront", "9b1e77c0aa", "Round order lines to cents")
                .file("src/api/orders.ts"),
            model: json!({"action": "reply", "text": "Which endpoint do you mean?"}),
            out: Out::Reply,
            has: vec!["src/api/orders.ts"],
            not: vec!["Which endpoint do you mean"],
        },
        Case {
            name: "generic lines are taken out",
            place: storefront(),
            view: View {
                said: vec![],
                heard: vec![heard("303", "kim-ito", "The empty-cart state still shows a zero total.", T0)],
            },
            say: Say::reply_all("feedback", "The empty cart now shows \"Your cart is empty\" instead of a zero total."),
            facts: Facts::default()
                .github_pr("northwind/storefront", 52, "", "open")
                .github_commit("northwind/storefront", "51c0de9f2b", "Empty cart message")
                .file("src/components/CartSummary.tsx"),
            model: json!({"action": "reply", "text": "Thanks for your comment! I'll look into it."}),
            out: Out::Reply,
            has: vec!["Your cart is empty"],
            not: vec!["Thanks for your comment", "look into it"],
        },
        Case {
            name: "a thank-you gets a reaction, not a comment",
            place: storefront(),
            view: View {
                said: vec![said("110", "feedback", "Removed the banner from the checkout page.", &[], T0)],
                heard: vec![heard("304", "dana-reyes", "thanks, looks good!", T0 + 60_000)],
            },
            say: Say::reply_all("feedback", "Nothing more to change here."),
            facts: Facts::default().github_pr("northwind/storefront", 52, "", "open"),
            model: json!({"action": "react", "text": ""}),
            out: Out::React,
            has: vec![],
            not: vec![],
        },
        Case {
            name: "people talking to each other get an acknowledgement, no words",
            place: storefront(),
            view: View {
                said: vec![],
                heard: vec![heard("305", "lee-park", "lol the staging deploy was slow for me too", T0)],
            },
            say: Say::reply_all("feedback", "No change needed for this comment."),
            facts: Facts::default().github_pr("northwind/storefront", 52, "", "open"),
            model: json!({"action": "quiet", "text": ""}),
            out: Out::React,
            has: vec![],
            not: vec![],
        },
        Case {
            name: "a vague comment gets a short relevant answer",
            place: Place::jira("SHOP-240"),
            view: View {
                said: vec![said("c7", "run", "Opened #57 for this ticket.", &[], T0)],
                heard: vec![Heard {
                    url: String::new(),
                    ..heard("j20", "Ana Ruiz", "hmm", T0 + 60_000)
                }],
            },
            say: Say::status("comment-received", "I'm working on this ticket now. I read this comment at my next step and act on it."),
            facts: Facts::default()
                .github_pr("northwind/storefront", 57, "", "open")
                .state("a run is working on this ticket; it reads this comment at its next step and acts on it"),
            model: json!({"action": "reply", "text": "Nothing in it changes the plan, so the work on #57 goes on as it is."}),
            out: Out::Reply,
            has: vec!["#57"],
            not: vec!["?", "Thanks"],
        },
        Case {
            name: "the one question the run needs stays",
            place: storefront(),
            view: View {
                said: vec![],
                heard: vec![heard("306", "priya-n", "Add the promo banner to the catalog page too.", T0)],
            },
            say: Say::reply_all("feedback", "Adding the promo banner to the catalog page.")
                .asks(vec!["Should the promo banner show on mobile too?".into()]),
            facts: Facts::default().github_pr("northwind/storefront", 52, "", "open"),
            model: json!({"action": "reply", "text": "I'm adding the promo banner to the catalog page. One call from you: should the promo banner show on mobile too?"}),
            out: Out::Reply,
            has: vec!["promo banner", "mobile too?"],
            not: vec![],
        },
    ]
}

/// The general rubric: what no reply may be.
fn rubric(text: &str, case: &Case, view: &View) -> Vec<String> {
    let mut bad = Vec::new();
    let l = text.to_lowercase();
    for g in [
        "thanks for your comment",
        "thank you for your comment",
        "let me know if",
        "please review",
        "a person should",
        "i read your comment",
    ] {
        if l.contains(g) {
            bad.push(format!("generic or hand-back: {g}"));
        }
    }
    if text.chars().any(cv::is_arrow) {
        bad.push("an arrow glyph".into());
    }
    // The thread rules: no header, one mention, no link to the thread, no
    // request said back.
    for p in common::comment_style::lint_on(text, &case.place) {
        bad.push(format!("thread rule: {p}"));
    }
    if text.chars().count() > rc::MAX_REPLY_CHARS {
        bad.push(format!("{} characters", text.chars().count()));
    }
    // Every claim in what was written is borne out: the check takes nothing
    // out of it again.
    let g = rc::Grounds::new(&case.facts, &case.say.body, view, &case.say.asks);
    let (_, dropped) = rc::check(text, &g);
    for (s, why) in dropped {
        bad.push(format!("unrecorded: {s} ({why})"));
    }
    bad
}

/// What was written to the person, without the quote the layer opens a
/// reply to an older comment with (old replies also carried a header).
fn reply_text(entry: &str) -> String {
    let body = entry.split_once(": ").map(|(_, b)| b).unwrap_or(entry);
    let mut lines: Vec<&str> = body.lines().collect();
    if lines.first().is_some_and(|l| l.starts_with("> ")) {
        lines.remove(0);
    }
    let text = lines.join("\n");
    let text = text.trim();
    // "@name, replying to [your comment](...)." or "Name, replying to ...".
    match text.find(".\n\n") {
        Some(at) if text[..at].contains("replying to") => text[at + 3..].to_string(),
        _ => text.to_string(),
    }
}

async fn run(case: &Case, composer: &dyn Composer) -> (Thread, Result<Spoke, String>) {
    let ch = Thread::new(case.place.clone(), case.view.clone());
    let c = Compose {
        composer,
        facts: &case.facts,
    };
    let out = cv::speak_with(&ch, None, Some(c), &case.say, T0 + 7_200_000).await;
    (ch, out)
}

fn grade(case: &Case, ch: &Thread, out: &Result<Spoke, String>, strict: bool) -> Vec<String> {
    let mut bad = Vec::new();
    let written = ch.written();
    match case.out {
        Out::React => {
            if !written.is_empty() {
                bad.push(format!("wrote {written:?} where a reaction was enough"));
            }
            if !ch.log().iter().any(|l| l.starts_with("react ")) {
                bad.push("the person's comment got no acknowledgement".into());
            }
        }
        Out::Reply => {
            let replies: Vec<&String> =
                written.iter().filter(|l| l.starts_with("reply ")).collect();
            if replies.len() != 1 || written.len() != 1 {
                bad.push(format!("expected one reply, got {written:?} ({out:?})"));
                return bad;
            }
            let full = replies[0].as_str();
            for h in &case.has {
                if strict && !full.contains(h) {
                    bad.push(format!("missing \"{h}\""));
                }
            }
            let text = reply_text(full);
            for n in &case.not {
                if text.contains(n) {
                    bad.push(format!("says \"{n}\""));
                }
            }
            bad.extend(rubric(&text, case, &case.view));
        }
    }
    bad
}

#[tokio::test]
async fn every_case_passes_with_the_recorded_answers() {
    let mut failed = Vec::new();
    for case in cases() {
        let model = Recorded::new(case.model.clone());
        let (ch, out) = run(&case, &model).await;
        let bad = grade(&case, &ch, &out, true);
        if !bad.is_empty() {
            failed.push(format!(
                "{}:\n  {}\n  log: {:?}",
                case.name,
                bad.join("\n  "),
                ch.log()
            ));
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n\n"));
}

/// The real shape: the answer under CoderHelm's question is applied,
/// confirmed in a reply, and the question reads answered where it was
/// asked.
#[tokio::test]
async fn an_answer_under_the_question_is_confirmed_and_marked_answered() {
    let mut case = cases().remove(0);
    case.place = under(&case.place, "answered");
    let model = Recorded::new(case.model.clone());
    let (ch, _) = run(&case, &model).await;
    cv::mark_answered(
        &ch,
        H1_QUESTION,
        "dana-reyes",
        "Skip the H1 and intro copy. Just use the title Spring Catalog",
    )
    .await
    .unwrap();
    let log = ch.log();
    let edit = log
        .iter()
        .find(|l| l.starts_with("edit 100"))
        .expect("question marked");
    assert!(edit.contains("(Answered by dana-reyes"), "{edit}");
    assert!(log.iter().any(|l| l.starts_with("reply 201")), "{log:?}");
}

/// A retried step answers nobody twice and asks the model once.
#[tokio::test]
async fn a_duplicate_retry_writes_nothing_new() {
    for mut case in cases().into_iter().take(3) {
        case.place = under(&case.place, "retry");
        let model = Recorded::new(case.model.clone());
        let ch = Thread::new(case.place.clone(), case.view.clone());
        for i in 0..3 {
            let c = Compose {
                composer: &model,
                facts: &case.facts,
            };
            cv::speak_with(&ch, None, Some(c), &case.say, T0 + 7_200_000 + i * 600_000)
                .await
                .unwrap();
        }
        let new: Vec<String> = ch
            .written()
            .into_iter()
            .filter(|l| l.starts_with("reply ") || l.starts_with("create "))
            .collect();
        assert_eq!(new.len(), 1, "{}: {new:?}", case.name);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1, "{}", case.name);
    }
}

/// The composer's answer is cached per comment and draft: asked again for
/// the same comment (a retried post), the model isn't.
#[tokio::test]
async fn the_reply_is_cached_per_comment() {
    let mut case = cases().remove(2);
    case.place = under(&case.place, "cache");
    let model = Recorded::new(case.model.clone());
    let to = case.view.heard[0].clone();
    let meta = Meta {
        slot: case.say.slot.clone(),
        fp: cv::fingerprint(&case.say.body),
        ..Default::default()
    };
    let c = Compose {
        composer: &model,
        facts: &case.facts,
    };
    let a = rc::composed(c, &case.view, Some(&to), &case.say, &case.place, &meta).await;
    let b = rc::composed(c, &case.view, Some(&to), &case.say, &case.place, &meta).await;
    assert_eq!(a, b);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
}

/// The review comments of one pull request, as GitHub lists them.
fn review_comments() -> Vec<Value> {
    vec![
        json!({
            "id": 900, "user": {"login": "kim-ito", "type": "User"},
            "body": "why is this file changed?",
            "path": "src/lib/currency.ts", "line": 18,
            "diff_hunk": "@@ -10,7 +10,9 @@ export function total(lines) {\n-  return lines.reduce((s, l) => s + l.price * l.qty, 0);\n+  return lines.reduce((s, l) => s + roundCents(l.price * l.qty), 0);",
            "created_at": "2026-11-14T10:00:00Z",
            "html_url": "https://github.com/northwind/storefront/pull/52#discussion_r900"
        }),
        json!({
            "id": 950, "user": {"login": "lee-park", "type": "User"},
            "body": "unrelated thread", "path": "README.md", "line": 2,
            "created_at": "2026-11-14T09:00:00Z"
        }),
    ]
}

/// The real shape: a reviewer asks "why is this file changed?" on a line.
/// The reply is in the thread, explains it from the diff under the comment
/// and the recorded decision, and asks nothing back.
#[tokio::test]
async fn why_is_this_file_changed_is_answered_from_the_diff() {
    let (view, to, code) = super::conversation::review_thread(&review_comments(), 900);
    let to = to.expect("the comment answered");
    assert!(to.line);
    assert_eq!(view.heard.len(), 1, "only its own thread: {view:?}");
    let (path, line, hunk) = code.unwrap();
    let facts = Facts::default()
        .github_pr("northwind/storefront", 52, "Cart totals match the invoice", "open")
        .decision("Round each line to cents before summing so the cart total matches the invoice (SHOP-214)")
        .code(&path, line, &hunk);
    let draft = "This file changed to round line totals.";
    let say = Say::reply("review-thread", draft, vec![to.id.clone()]);
    let place = under(&storefront(), "thread");
    let model = Recorded::new(
        json!({"action": "reply", "text": "It's part of the cart total fix: each line is rounded to cents with `roundCents` before it's summed, so the total matches the invoice SHOP-214 describes. Before, `total` summed unrounded line prices and drifted by a cent."}),
    );
    let meta = Meta {
        slot: "review-thread".into(),
        fp: cv::fingerprint(draft),
        ..Default::default()
    };
    let d = rc::apply(
        Compose {
            composer: &model,
            facts: &facts,
        },
        &view,
        &say,
        &place,
        Decision::Reply {
            to: to.clone(),
            others: vec![to.clone()],
            how: How::Thread,
            ack: String::new(),
            text: draft.into(),
            meta,
        },
    )
    .await;
    let Decision::Reply { text, how, .. } = d else {
        panic!("{d:?}")
    };
    assert_eq!(how, How::Thread);
    assert!(
        text.contains("roundCents") && text.contains("invoice"),
        "{text}"
    );
    assert!(!text.contains('?'), "{text}");
    let g = rc::Grounds::new(&facts, draft, &view, &[]);
    assert!(rc::check(&text, &g).1.is_empty(), "{text}");
}

/// The live model on the same threads: the rubric and the outcome, not the
/// exact words.
#[tokio::test]
#[ignore = "calls the model: ANTHROPIC_API_KEY=... cargo test -p worker reply_eval -- --ignored"]
async fn live_model_writes_the_replies() {
    let Ok(key) = std::env::var("ANTHROPIC_API_KEY") else {
        eprintln!("ANTHROPIC_API_KEY is not set: the live eval is skipped");
        return;
    };
    struct Live(crate::agent::anthropic::AnthropicClient);
    #[async_trait::async_trait]
    impl Composer for Live {
        async fn compose(&self, system: &str, prompt: &str, schema: &Value) -> Option<Value> {
            let mut usage = crate::models::TokenUsage::default();
            crate::agent::anthropic::converse_json(
                &self.0,
                &std::env::var("REPLY_EVAL_MODEL").unwrap_or_else(|_| "claude-sonnet-5-5".into()),
                system,
                prompt,
                schema,
                true,
                "low",
                rc::MAX_TOKENS,
                &mut usage,
            )
            .await
            .map_err(|e| eprintln!("model call failed: {e}"))
            .ok()
        }
    }
    let live = Live(crate::agent::anthropic::AnthropicClient::new(
        key,
        common::usage::UsageMeter::memory("reply-eval").0,
    ));
    let mut failed = Vec::new();
    for mut case in cases() {
        case.place = under(&case.place, "live");
        let (ch, out) = run(&case, &live).await;
        eprintln!("--- {}\n{:?}\n", case.name, ch.log());
        let bad = grade(&case, &ch, &out, false);
        if !bad.is_empty() {
            failed.push(format!("{}:\n  {}", case.name, bad.join("\n  ")));
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n\n"));
}
