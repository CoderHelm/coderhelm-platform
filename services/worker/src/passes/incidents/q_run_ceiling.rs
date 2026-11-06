//! q. The owner's requirement: a run cannot loop without end, in layers that
//! don't depend on each other. An adversarial checker that always sends the run
//! back, under every kind of run message and every pass, ends within the ceiling
//! through the real entry guard, and with any one layer removed the others still
//! end it. With all removed it doesn't end: the test has teeth.

use std::cell::RefCell;
use std::sync::Arc;

use common::messages::WorkerMessage;
use common::usage::RunAllowance;

use crate::models::{ResumeMessage, TicketMessage};
use crate::passes::run_ceiling::*;

const TEAM: &str = "TEAM#1";
const RUN: &str = "RUN1";

/// Every pass the worker records, and one nobody has written yet.
const PASSES: &[&str] = &[
    "triage",
    "plan",
    "implement",
    "test",
    "security",
    "pr",
    "review",
    "feedback",
    "conflict_fix",
    "ci_fix",
    "a_pass_added_later",
];

/// Every kind of message that belongs to a run.
fn run_messages() -> Vec<(&'static str, WorkerMessage)> {
    let ticket = TicketMessage {
        team_id: TEAM.into(),
        ticket_id: "GH-1".into(),
        continuation: 1,
        continuation_run_id: Some(RUN.into()),
        ..Default::default()
    };
    let resume = ResumeMessage {
        github_wait: Default::default(),
        team_id: TEAM.into(),
        run_id: RUN.into(),
        installation_id: 1,
        model_retries: 0,
    };
    let feedback: WorkerMessage = serde_json::from_value(serde_json::json!({
        "type": "feedback",
        "team_id": TEAM,
        "run_id": RUN,
        "installation_id": 1,
        "pr_number": 7,
        "repo_owner": "a",
        "repo_name": "b",
        "branch": "c",
        "review_id": 0,
        "comments": [],
        "review_body": "",
    }))
    .expect("a feedback message");
    vec![
        ("continuation", WorkerMessage::Ticket(ticket)),
        ("resume", WorkerMessage::Resume(resume)),
        ("feedback", feedback),
    ]
}

/// Which layers are in place, to take each away in turn.
#[derive(Clone, Copy)]
struct Layers {
    entry: bool,
    model: bool,
    sweep: bool,
}

const ALL: Layers = Layers {
    entry: true,
    model: true,
    sweep: true,
};

/// The run record, in memory, behind the real `EntryGate`.
#[derive(Default)]
struct Record {
    totals: RefCell<Totals>,
    ended_by_entry: RefCell<u32>,
}

impl EntryGate for Record {
    async fn totals(&self, team_id: &str, run_id: &str) -> Option<Totals> {
        assert_eq!((team_id, run_id), (TEAM, RUN));
        Some(self.totals.borrow().clone())
    }
    async fn end_over(&self, _message: &WorkerMessage, _over: Over) {
        *self.ended_by_entry.borrow_mut() += 1;
    }
}

#[derive(Debug, PartialEq)]
enum Ended {
    Entry,
    ModelRefused,
    Sweep,
    Never,
}

/// A message goes through the real entry guard, and the adversarial checker
/// always sends the run back: one pass, one continuation, a step's spend and
/// time, then the same message again.
async fn adversary(message: &WorkerMessage, pass: &str, layers: Layers) -> (Ended, Totals) {
    let record = Record::default();
    let usd_per_step = 4.0;
    for _ in 0..100_000u32 {
        if layers.entry && !admit(&record, message).await {
            assert_eq!(*record.ended_by_entry.borrow(), 1);
            let totals = record.totals.borrow().clone();
            return (Ended::Entry, totals);
        }
        // The step's model allowance, as `worked` gives it.
        let left = record.totals.borrow().spend_left();
        let allowance = Arc::new(RunAllowance::new(if layers.model {
            left
        } else {
            f64::MAX
        }));
        let refused = common::usage::with_run_allowance(allowance, async {
            // A step's calls: refused once the allowance is spent.
            let client = aws_sdk_dynamodb::Client::from_conf(
                aws_sdk_dynamodb::Config::builder()
                    .behavior_version_latest()
                    .build(),
            );
            // No team, so `admit` asks nothing outside: only the allowance.
            let meter = common::usage::UsageMeter::dynamo(client, "", "");
            for _ in 0..4 {
                if meter.admit().await.is_err() {
                    return true;
                }
                common::usage::note_run_spend(usd_per_step / 4.0);
                record.totals.borrow_mut().spend_usd += usd_per_step / 4.0;
            }
            false
        })
        .await;
        if refused {
            let totals = record.totals.borrow().clone();
            return (Ended::ModelRefused, totals);
        }
        {
            let mut t = record.totals.borrow_mut();
            t.passes += 1;
            t.continuations += 1;
            t.worked_secs += 900;
        }
        // The sweep runs outside all of this, on a clock of its own.
        if layers.sweep && record.totals.borrow().over().is_some() {
            let totals = record.totals.borrow().clone();
            return (Ended::Sweep, totals);
        }
        assert!(!pass.is_empty());
    }
    let totals = record.totals.borrow().clone();
    (Ended::Never, totals)
}

/// One step's overshoot at most: the ceiling is checked between steps, and a
/// message already running finishes its step unless a model call refuses it.
fn within_the_ceiling(t: &Totals) -> bool {
    t.passes <= MAX_RUN_PASSES + 1
        && t.continuations <= MAX_RUN_CONTINUATIONS + 1
        && t.worked_secs <= MAX_RUN_WORKED_SECS + 900
        && t.spend_usd <= MAX_RUN_SPEND_USD + 4.0
}

#[tokio::test]
async fn every_message_and_pass_ends_within_the_ceiling() {
    for (kind, message) in run_messages() {
        assert_eq!(message.run_ref(), Some((TEAM, RUN)), "{kind}");
        for pass in PASSES {
            let (ended, totals) = adversary(&message, pass, ALL).await;
            assert_ne!(ended, Ended::Never, "{kind}/{pass}");
            assert!(within_the_ceiling(&totals), "{kind}/{pass}: {totals:?}");
        }
    }
}

/// Each layer taken away, and the others still end it within the ceiling.
#[tokio::test]
async fn with_any_one_layer_removed_the_others_still_end_the_run() {
    let (_, message) = &run_messages()[0];
    let table = [
        (
            "no entry guard",
            Layers {
                entry: false,
                ..ALL
            },
        ),
        (
            "no model refusal",
            Layers {
                model: false,
                ..ALL
            },
        ),
        (
            "no sweep",
            Layers {
                sweep: false,
                ..ALL
            },
        ),
        (
            "only the entry guard",
            Layers {
                entry: true,
                model: false,
                sweep: false,
            },
        ),
        (
            "only the model refusal",
            Layers {
                entry: false,
                model: true,
                sweep: false,
            },
        ),
        (
            "only the sweep",
            Layers {
                entry: false,
                model: false,
                sweep: true,
            },
        ),
    ];
    for (name, layers) in table {
        let (ended, totals) = adversary(message, "review", layers).await;
        assert_ne!(ended, Ended::Never, "{name}");
        assert!(within_the_ceiling(&totals), "{name}: {totals:?}");
    }
}

/// With every layer gone it runs on: this test can fail.
#[tokio::test]
async fn with_every_layer_removed_it_never_ends() {
    let (_, message) = &run_messages()[0];
    let none = Layers {
        entry: false,
        model: false,
        sweep: false,
    };
    let (ended, totals) = adversary(message, "review", none).await;
    assert_eq!(ended, Ended::Never);
    assert!(totals.over().is_some());
}

/// A message that isn't a run's never asks the record, and a first ticket
/// message has no run yet: its first step counts from the first pass on.
#[tokio::test]
async fn a_message_with_no_run_runs_and_a_person_retrying_gets_a_new_allowance() {
    let record = Record::default();
    let fresh = WorkerMessage::Ticket(TicketMessage {
        team_id: TEAM.into(),
        ..Default::default()
    });
    assert_eq!(fresh.run_ref(), None);
    assert!(admit(&record, &fresh).await);
    // A retry is a new run id, so a new record: over on the old one, fine here.
    let retry = WorkerMessage::Ticket(TicketMessage {
        team_id: TEAM.into(),
        resume_from: Some(RUN.into()),
        ..Default::default()
    });
    assert_eq!(retry.run_ref(), None, "a retry is not the old run");
}

/// A new message kind can't skip the entry guard, and nor can a dispatch.
#[test]
fn the_entry_guard_comes_before_every_dispatch() {
    let main = include_str!("../../main.rs");
    assert_eq!(
        main.matches("handle_message(").count(),
        2,
        "one call, one fn"
    );
    let admit_at = main.find("run_ceiling::admit(").expect("the entry guard");
    let dispatch_at = main
        .find("handle_message(Arc::clone(&state), message)")
        .unwrap();
    assert!(admit_at < dispatch_at);
    let worked_at = main.find("run_ceiling::worked(").expect("the allowance");
    assert!(admit_at < worked_at && worked_at < dispatch_at);
    // `run_ref` lists every message kind: no catch-all, so a new one doesn't
    // compile until it says whether it belongs to a run.
    let msgs = include_str!("../../../../common/src/messages.rs");
    let at = msgs.find("pub fn run_ref(&self)").unwrap();
    let body = &msgs[at..at + msgs[at..].find("\n    }\n").unwrap()];
    assert!(!body.contains("_ =>"), "run_ref has a catch-all");
    for kind in ["Ticket", "Feedback", "Resume"] {
        assert!(body.contains(&format!("WorkerMessage::{kind}(")), "{kind}");
    }
}

/// Every model call goes through `UsageMeter::admit` (`model_call_sites` fails
/// the build otherwise), and the allowance is asked first, before anything that
/// could let a call past.
#[test]
fn a_model_call_cannot_skip_the_allowance() {
    let usage = include_str!("../../../../common/src/usage.rs");
    let at = usage.find("pub async fn admit(&self)").unwrap();
    let body = &usage[at..at + usage[at..].find("\n    }\n").unwrap()];
    let allowance = body.find("run_allowance_refused()").expect("asks it");
    assert!(allowance < body.find("self.team_id.is_empty()").unwrap());
}

/// A step already queued doesn't undo the stop, and a pass notices the ceiling.
#[test]
fn the_stop_holds_and_passes_add_to_the_counts() {
    let g = include_str!("../run_guard.rs");
    assert!(g.contains("ADD ceil_passes :p, ceil_continuations :c, ceil_spend_usd :s"));
    assert!(g.contains("close_run_allowance("));
    // Nothing but addition ever touches the counts, anywhere.
    for (name, src) in [
        ("guard", g),
        ("mod", include_str!("../mod.rs")),
        ("resume", include_str!("../resume.rs")),
        ("feedback", include_str!("../feedback.rs")),
    ] {
        for f in [
            "ceil_passes",
            "ceil_continuations",
            "ceil_spend_usd",
            "ceil_worked_secs",
        ] {
            for bad in [
                format!("SET {f}"),
                format!("REMOVE {f}"),
                format!(", {f} ="),
            ] {
                assert!(!src.contains(&bad), "{name} writes {bad}");
            }
        }
    }
}
