//! The gate end to end, on the in-memory store: asks and answers, the
//! timeout, the pause, sign-in prompts and team isolation.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use super::approvals::{self, Approver, Choice, DecideError, Status, Via};
use super::gate::{Gate, GateCtx, Notifier, Outcome, Request, Stop, Wait};
use super::model::*;
use super::pause::{self, Resender};
use super::signin::{self, Need};
use super::store::{MemStore, PolicyStore};

const TEAM: &str = "TEAM#a";
const OTHER: &str = "TEAM#b";

fn admin() -> Approver {
    Approver {
        email: "admin@x.dev".into(),
        can_approve: true,
        can_edit_rules: true,
        api_key: false,
    }
}
fn member() -> Approver {
    Approver {
        email: "member@x.dev".into(),
        can_approve: true,
        can_edit_rules: false,
        api_key: false,
    }
}

/// A policy with no rules: the engine's safe defaults (writes ask).
async fn bare(store: &MemStore, team: &str) {
    let p = TeamPolicy {
        version: 1,
        ..Default::default()
    };
    store.save(team, &p, 0).await.unwrap();
}

#[derive(Default)]
struct Calls(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl Notifier for Calls {
    async fn approval_needed(&self, _team: &str, a: &approvals::Approval) {
        self.0.lock().unwrap().push(a.id.clone());
    }
}

fn run_gate(store: Arc<MemStore>, team: &str) -> Gate {
    Gate::new(
        store,
        GateCtx::new(team, Agent::Coder)
            .repo("acme/web")
            .run("run-1")
            .task("Fix the login bug"),
    )
    .with_wait(Wait::Block {
        max: Duration::from_secs(600),
        poll: Duration::from_millis(5),
    })
}

fn write_req(args: &Value) -> Request<'_> {
    Request {
        tool: "write_file",
        args,
        why: "Fix the null check",
    }
}

async fn pending_id(store: &MemStore, team: &str) -> String {
    for _ in 0..400 {
        if let Some(a) = store.list_approvals(team, true).await.unwrap().first() {
            return a.id.clone();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("no approval card was raised");
}

#[tokio::test]
async fn a_new_team_starts_on_standard() {
    let store = Arc::new(MemStore::new());
    let gate = run_gate(store.clone(), TEAM);
    let args = json!({"path": "a.rs", "content": "x"});
    assert!(gate.check(write_req(&args)).await.runs());
    let send = json!({"text": "hi"});
    let g = Gate::new(
        store.clone(),
        GateCtx::new(TEAM, Agent::PlanChat).thread("t1"),
    );
    match g
        .check(Request {
            tool: "slack__send_message",
            args: &send,
            why: "",
        })
        .await
    {
        Outcome::Pending { .. } => {}
        other => panic!("expected an ask, got {other:?}"),
    }
}

#[tokio::test]
async fn approve_once_runs_this_call_only() {
    let store = Arc::new(MemStore::new());
    bare(&store, TEAM).await;
    let notes = Arc::new(Calls::default());
    let gate = run_gate(store.clone(), TEAM).with_notifier(notes.clone());
    let args = json!({"path": "a.rs", "content": "x", "api_token": "abc"});

    let s2 = store.clone();
    let answer = tokio::spawn(async move {
        let id = pending_id(&s2, TEAM).await;
        let a = s2.get_approval(TEAM, &id).await.unwrap().unwrap();
        // The card shows the exact arguments with secrets hidden, the why and the risk.
        assert_eq!(a.args["path"], "a.rs");
        assert_eq!(a.args["api_token"], "(hidden)");
        assert_eq!(a.why, "Fix the null check");
        assert_eq!(a.risk, Some(Risk::Write));
        approvals::decide(
            &*s2,
            TEAM,
            &id,
            &member(),
            true,
            Choice::Once,
            Via::RunPage,
            "",
            &super::now(),
        )
        .await
        .unwrap()
    });
    let out = gate.check(write_req(&args)).await;
    let decided = answer.await.unwrap();
    assert!(out.runs(), "{out:?}");
    assert_eq!(decided.status, Status::Approved);
    assert_eq!(decided.decided_via, Some(Via::RunPage));
    assert_eq!(notes.0.lock().unwrap().len(), 1);
    // No rule was written.
    assert!(store.load(TEAM).await.unwrap().rules.is_empty());
    // The decision is in the log with what decided it.
    let ev = store.events_for(TEAM);
    assert!(ev.iter().any(|e| e.action == "tool.queued_for_approval"));
    assert!(ev.iter().any(|e| e.action == "approval.approved"));

    // A different call asks again.
    let other = json!({"path": "b.rs", "content": "y"});
    let fast = Gate::new(
        store.clone(),
        GateCtx::new(TEAM, Agent::Coder)
            .repo("acme/web")
            .run("run-1"),
    );
    assert!(matches!(
        fast.check(write_req(&other)).await,
        Outcome::Pending { .. }
    ));
}

#[tokio::test]
async fn a_chat_retries_after_approve_once() {
    let store = Arc::new(MemStore::new());
    bare(&store, TEAM).await;
    let g = Gate::new(
        store.clone(),
        GateCtx::new(TEAM, Agent::PlanChat).thread("t1"),
    );
    let args = json!({"title": "Bug"});
    let req = Request {
        tool: "linear__create_issue",
        args: &args,
        why: "",
    };
    let Outcome::Pending { approval, .. } = g.check(req).await else {
        panic!("expected an ask")
    };
    assert_eq!(approval.thread_id, "t1");
    // Asking again before an answer reuses the same card.
    let Outcome::Pending {
        approval: again, ..
    } = g.check(req).await
    else {
        panic!("expected an ask")
    };
    assert_eq!(again.id, approval.id);
    approvals::decide(
        &*store,
        TEAM,
        &approval.id,
        &member(),
        true,
        Choice::Once,
        Via::Chat,
        "",
        &super::now(),
    )
    .await
    .unwrap();
    assert!(g.check(req).await.runs());
    // Once means once.
    assert!(matches!(g.check(req).await, Outcome::Pending { .. }));
}

#[tokio::test]
async fn approve_always_writes_a_visible_rule() {
    let store = Arc::new(MemStore::new());
    bare(&store, TEAM).await;
    let g = Gate::new(
        store.clone(),
        GateCtx::new(TEAM, Agent::Coder)
            .repo("acme/web")
            .run("run-2"),
    );
    let args = json!({"path": "a.rs"});
    let Outcome::Pending { approval, .. } = g.check(write_req(&args)).await else {
        panic!("expected an ask")
    };
    // Someone who can't edit rules can't choose always.
    let e = approvals::decide(
        &*store,
        TEAM,
        &approval.id,
        &member(),
        true,
        Choice::AlwaysRepo,
        Via::Chat,
        "",
        &super::now(),
    )
    .await
    .unwrap_err();
    assert!(matches!(e, DecideError::NotAllowed(_)));
    let a = approvals::decide(
        &*store,
        TEAM,
        &approval.id,
        &admin(),
        true,
        Choice::AlwaysRepo,
        Via::Slack,
        "fine",
        &super::now(),
    )
    .await
    .unwrap();
    let p = store.load(TEAM).await.unwrap();
    assert_eq!(p.rules.len(), 1);
    let r = &p.rules[0];
    assert_eq!(r.id, a.rule_id);
    assert_eq!(r.scope, Scope::Repo);
    assert_eq!(r.repo.as_deref(), Some("acme/web"));
    assert_eq!(r.tool.as_deref(), Some("write_file"));
    assert_eq!(r.decision, Decision::Allow);
    assert_eq!(r.source, RuleSource::Approval);
    assert!(r.note.contains("admin@x.dev"));
    // Any later write in that repository runs; another repository still asks.
    let g2 = Gate::new(
        store.clone(),
        GateCtx::new(TEAM, Agent::Coder)
            .repo("acme/web")
            .run("run-3"),
    );
    let Outcome::Run { verdict } = g2.check(write_req(&json!({"path": "z.rs"}))).await else {
        panic!("expected run")
    };
    assert_eq!(verdict.decided_by.rule_id(), Some(r.id.as_str()));
    let g3 = Gate::new(
        store.clone(),
        GateCtx::new(TEAM, Agent::Coder)
            .repo("acme/api")
            .run("run-4"),
    );
    assert!(matches!(
        g3.check(write_req(&json!({"path": "z.rs"}))).await,
        Outcome::Pending { .. }
    ));
    assert!(store
        .events_for(TEAM)
        .iter()
        .any(|e| e.action == "rule.added"));

    // Always for this tool: a team rule.
    let Outcome::Pending { approval, .. } = g3.check(write_req(&json!({"path": "q.rs"}))).await
    else {
        panic!("expected an ask")
    };
    approvals::decide(
        &*store,
        TEAM,
        &approval.id,
        &admin(),
        true,
        Choice::AlwaysTool,
        Via::Teams,
        "",
        &super::now(),
    )
    .await
    .unwrap();
    let p = store.load(TEAM).await.unwrap();
    assert!(p
        .rules
        .iter()
        .any(|r| r.scope == Scope::Team && r.tool.as_deref() == Some("write_file")));
}

#[tokio::test]
async fn always_is_refused_above_the_cap_but_once_still_works() {
    let store = Arc::new(MemStore::new());
    bare(&store, TEAM).await;
    let g = Gate::new(
        store.clone(),
        GateCtx::new(TEAM, Agent::PlanChat).thread("t9"),
    );
    let args = json!({"channel": "#general"});
    let req = Request {
        tool: "slack__send_message",
        args: &args,
        why: "",
    };
    let Outcome::Pending { approval, .. } = g.check(req).await else {
        panic!("expected an ask")
    };
    let e = approvals::decide(
        &*store,
        TEAM,
        &approval.id,
        &admin(),
        true,
        Choice::AlwaysTool,
        Via::Chat,
        "",
        &super::now(),
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("autonomy limit"));
    approvals::decide(
        &*store,
        TEAM,
        &approval.id,
        &admin(),
        true,
        Choice::Once,
        Via::Chat,
        "",
        &super::now(),
    )
    .await
    .unwrap();
    assert!(g.check(req).await.runs());
}

#[tokio::test]
async fn deny_tells_the_model_and_never_runs() {
    let store = Arc::new(MemStore::new());
    bare(&store, TEAM).await;
    let gate = run_gate(store.clone(), TEAM);
    let s2 = store.clone();
    let answer = tokio::spawn(async move {
        let id = pending_id(&s2, TEAM).await;
        approvals::decide(
            &*s2,
            TEAM,
            &id,
            &member(),
            false,
            Choice::Once,
            Via::Email,
            "Not on main",
            &super::now(),
        )
        .await
        .unwrap()
    });
    let args = json!({"path": "a.rs"});
    let out = gate.check(write_req(&args)).await;
    answer.await.unwrap();
    match out {
        Outcome::Refused { message, .. } => {
            assert!(message.contains("member@x.dev declined"));
            assert!(message.contains("Not on main"));
        }
        other => panic!("expected refused, got {other:?}"),
    }
    // Answering twice is refused.
    let id = store.list_approvals(TEAM, false).await.unwrap()[0]
        .id
        .clone();
    let e = approvals::decide(
        &*store,
        TEAM,
        &id,
        &member(),
        true,
        Choice::Once,
        Via::RunPage,
        "",
        &super::now(),
    )
    .await
    .unwrap_err();
    assert_eq!(e, DecideError::AlreadyDecided(Status::Denied));
}

#[tokio::test]
async fn an_unanswered_card_times_out_and_stops_cleanly() {
    let store = Arc::new(MemStore::new());
    bare(&store, TEAM).await;
    let gate = Gate::new(
        store.clone(),
        GateCtx::new(TEAM, Agent::Coder)
            .repo("acme/web")
            .run("run-t"),
    )
    .with_wait(Wait::Block {
        max: Duration::from_secs(1),
        poll: Duration::from_millis(20),
    });
    let args = json!({"path": "a.rs"});
    let out = gate.check(write_req(&args)).await;
    let Outcome::Stop(stop) = out else {
        panic!("expected a stop, got {out:?}")
    };
    let Stop::TimedOut { approval } = &stop else {
        panic!("expected a timeout")
    };
    assert_eq!(approval.status, Status::Expired);
    assert!(stop
        .note()
        .starts_with("Stopped: nobody answered the request to use write_file in time."));
    let stored = store
        .get_approval(TEAM, &approval.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, Status::Expired);
    // An answer after the timeout is refused.
    let e = approvals::decide(
        &*store,
        TEAM,
        &approval.id,
        &admin(),
        true,
        Choice::Once,
        Via::RunPage,
        "",
        &super::now(),
    )
    .await
    .unwrap_err();
    assert_eq!(e, DecideError::AlreadyDecided(Status::Expired));
    assert!(store
        .events_for(TEAM)
        .iter()
        .any(|e| e.action == "approval.expired"));
}

#[tokio::test]
async fn an_api_key_never_answers_a_card_even_with_every_capability() {
    let store = Arc::new(MemStore::new());
    bare(&store, TEAM).await;
    let g = Gate::new(store.clone(), GateCtx::new(TEAM, Agent::Coder).run("r"));
    let Outcome::Pending { approval, .. } = g.check(write_req(&json!({}))).await else {
        panic!("expected an ask")
    };
    let key = Approver {
        email: "ci-key".into(),
        can_approve: true,
        can_edit_rules: true,
        api_key: true,
    };
    for choice in [Choice::Once, Choice::AlwaysTool] {
        let e = approvals::decide(
            &*store,
            TEAM,
            &approval.id,
            &key,
            true,
            choice,
            Via::Settings,
            "",
            &super::now(),
        )
        .await
        .unwrap_err();
        assert!(matches!(e, DecideError::NotAllowed(_)));
    }
    assert_eq!(
        store
            .get_approval(TEAM, &approval.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        Status::Pending
    );
    assert!(store.load(TEAM).await.unwrap().rules.is_empty());
}

#[tokio::test]
async fn people_without_the_approve_capability_cannot_answer() {
    let store = Arc::new(MemStore::new());
    bare(&store, TEAM).await;
    let g = Gate::new(store.clone(), GateCtx::new(TEAM, Agent::Coder).run("r"));
    let Outcome::Pending { approval, .. } = g.check(write_req(&json!({}))).await else {
        panic!("expected an ask")
    };
    let viewer = Approver {
        email: "v@x.dev".into(),
        can_approve: false,
        can_edit_rules: false,
        api_key: false,
    };
    let e = approvals::decide(
        &*store,
        TEAM,
        &approval.id,
        &viewer,
        true,
        Choice::Once,
        Via::RunPage,
        "",
        &super::now(),
    )
    .await
    .unwrap_err();
    assert!(matches!(e, DecideError::NotAllowed(_)));
}

#[derive(Default)]
struct Sent(Mutex<Vec<Value>>);

#[async_trait::async_trait]
impl Resender for Sent {
    async fn send(&self, m: &Value) -> Result<(), String> {
        self.0.lock().unwrap().push(m.clone());
        Ok(())
    }
}

#[tokio::test]
async fn pause_all_keeps_work_and_resumes_it_in_order() {
    let store = Arc::new(MemStore::new());
    pause::pause(&*store, TEAM, "admin@x.dev", "Release freeze", None)
        .await
        .unwrap();

    // New work is kept, not run.
    let m1 = json!({"type": "ticket", "n": 1});
    let m2 = json!({"type": "ticket", "n": 2});
    assert_eq!(
        pause::hold_if_paused(&*store, TEAM, "Run 1", "run-1", &m1)
            .await
            .as_deref(),
        Some("Release freeze")
    );
    assert!(pause::hold_if_paused(&*store, TEAM, "Run 2", "run-2", &m2)
        .await
        .is_some());
    // Another team isn't paused.
    assert!(pause::hold_if_paused(&*store, OTHER, "Run x", "run-x", &m1)
        .await
        .is_none());

    // A loop already running stops at its next call.
    let g = Gate::new(
        store.clone(),
        GateCtx::new(TEAM, Agent::Planner).run("run-3"),
    );
    let out = g
        .check(Request {
            tool: "read_file",
            args: &json!({"path": "a"}),
            why: "",
        })
        .await;
    let Outcome::Stop(Stop::Paused { reason }) = out else {
        panic!("expected a pause stop")
    };
    assert_eq!(reason, "Release freeze");
    pause::keep(
        &*store,
        TEAM,
        "Run 3",
        "run-3",
        &json!({"type": "ticket", "n": 3}),
    )
    .await;

    // Lifting it sends everything again, oldest first, and empties the queue.
    let sent = Sent::default();
    let n = pause::lift(&*store, TEAM, "admin@x.dev", &sent)
        .await
        .unwrap();
    assert_eq!(n, 3);
    let order: Vec<i64> = sent
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|m| m["n"].as_i64().unwrap())
        .collect();
    assert_eq!(order, vec![1, 2, 3]);
    assert!(store.list_queued(TEAM).await.unwrap().is_empty());
    assert!(store.load(TEAM).await.unwrap().pause.is_none());
}

#[tokio::test]
async fn a_pause_with_an_until_time_resumes_on_its_own() {
    let store = Arc::new(MemStore::new());
    let past = (chrono::Utc::now() - chrono::Duration::seconds(5)).to_rfc3339();
    let future = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    pause::pause(&*store, TEAM, "a@x.dev", "Lunch", Some(future))
        .await
        .unwrap();
    pause::keep(&*store, TEAM, "Run 1", "run-1", &json!({"n": 1})).await;
    let sent = Sent::default();
    // Still paused: the sweep sends nothing.
    assert_eq!(pause::resume_queued(&*store, TEAM, &sent).await.unwrap(), 0);
    // Its time passes: the sweep sends the kept work.
    pause::pause(&*store, TEAM, "a@x.dev", "Lunch", Some(past))
        .await
        .unwrap();
    assert_eq!(pause::resume_queued(&*store, TEAM, &sent).await.unwrap(), 1);
}

#[tokio::test]
async fn sign_in_is_asked_once_per_connection_per_thread_and_never_after_no() {
    let store = Arc::new(MemStore::new());
    assert!(signin::should_prompt(&*store, TEAM, "chat-1", "figma").await);
    assert!(!signin::should_prompt(&*store, TEAM, "chat-1", "figma").await);
    // Another connection, or another thread, is its own prompt.
    assert!(signin::should_prompt(&*store, TEAM, "chat-1", "linear").await);
    assert!(signin::should_prompt(&*store, TEAM, "chat-2", "figma").await);
    // Declined: never asked again in that thread.
    signin::decline(&*store, TEAM, "chat-2", "figma", "ann@x.dev").await;
    assert!(signin::declined(&*store, TEAM, "chat-2", "figma").await);
    assert!(!signin::should_prompt(&*store, TEAM, "chat-2", "figma").await);
    signin::connected(&*store, TEAM, "chat-2", "figma").await;
    assert!(
        !signin::should_prompt(&*store, TEAM, "chat-2", "figma").await,
        "a no stays a no"
    );
    // Connecting ends the thread's prompt; a later break may be prompted again.
    signin::connected(&*store, TEAM, "chat-1", "figma").await;
    assert!(signin::should_prompt(&*store, TEAM, "chat-1", "figma").await);
    // Another team's thread with the same id is separate.
    assert!(signin::should_prompt(&*store, OTHER, "chat-2", "figma").await);
}

#[tokio::test]
async fn sign_in_prompts_say_who_can_fix_it() {
    let p = signin::prompt("figma", "Figma", Need::Personal, false, "read this file");
    assert_eq!(p.message, "Sign in to Figma to let me read this file.");
    assert_eq!(p.action_path, "/api/mcp-servers/figma/connect");
    let admin = signin::prompt("sentry", "Sentry", Need::TeamMissing, true, "");
    assert!(!admin.ask_admin);
    assert_eq!(admin.action_label, "Open settings");
    let member = signin::prompt("sentry", "Sentry", Need::TeamMissing, false, "");
    assert!(member.ask_admin);
    assert!(member.message.contains("Ask an admin"));
    assert!(member.action_path.is_empty());
}

#[tokio::test]
async fn teams_never_see_each_others_policy_cards_or_queue() {
    let store = Arc::new(MemStore::new());
    bare(&store, TEAM).await;
    // Team B allows writes everywhere.
    let mut b = TeamPolicy::starting();
    b.version = 1;
    store.save(OTHER, &b, 0).await.unwrap();

    let ga = Gate::new(store.clone(), GateCtx::new(TEAM, Agent::Coder).run("r"));
    let gb = Gate::new(store.clone(), GateCtx::new(OTHER, Agent::Coder).run("r"));
    let args = json!({"path": "a"});
    let Outcome::Pending { approval, .. } = ga.check(write_req(&args)).await else {
        panic!("team a asks")
    };
    assert!(gb.check(write_req(&args)).await.runs());
    // Team B can't see or answer team A's card, even with its id.
    assert!(store.list_approvals(OTHER, false).await.unwrap().is_empty());
    let e = approvals::decide(
        &*store,
        OTHER,
        &approval.id,
        &admin(),
        true,
        Choice::Once,
        Via::RunPage,
        "",
        &super::now(),
    )
    .await
    .unwrap_err();
    assert_eq!(e, DecideError::NotFound);
    // Team B's pause doesn't stop team A.
    pause::pause(&*store, OTHER, "b@x.dev", "B only", None)
        .await
        .unwrap();
    assert!(ga
        .check(Request {
            tool: "read_file",
            args: &args,
            why: ""
        })
        .await
        .runs());
    // Decisions are logged under their own team.
    assert!(store
        .events_for(OTHER)
        .iter()
        .all(|e| e.run_id.is_empty() || e.run_id == "r"));
    assert!(!store
        .events_for(TEAM)
        .iter()
        .any(|e| e.detail.contains("B only")));
}

#[tokio::test]
async fn a_tool_not_offered_to_the_model_is_unknown() {
    let store = Arc::new(MemStore::new());
    let g = Gate::new(store.clone(), GateCtx::new(TEAM, Agent::Reviewer).run("r"))
        .offering(["read_file"]);
    let args = json!({});
    assert!(g
        .check(Request {
            tool: "read_file",
            args: &args,
            why: ""
        })
        .await
        .runs());
    match g
        .check(Request {
            tool: "write_file",
            args: &args,
            why: "",
        })
        .await
    {
        Outcome::Refused { verdict, .. } => assert_eq!(verdict.decided_by, DecidedBy::Unknown),
        other => panic!("expected refused, got {other:?}"),
    }
}

#[tokio::test]
async fn sandbox_commands_go_through_the_gate() {
    let store = Arc::new(MemStore::new());
    // Standard lets the repository's own build and test commands run.
    let g = Gate::new(
        store.clone(),
        GateCtx::new(TEAM, Agent::Coder).repo("acme/web").run("r"),
    );
    assert!(g
        .check_command("cd web && npm ci && npm run build", "")
        .await
        .runs());

    // With no command rule (Cautious, or rules removed) only the allowlist runs.
    let mut p = TeamPolicy {
        version: 1,
        ..Default::default()
    };
    p.sandbox.commands.insert(
        "acme/web".into(),
        vec![AllowedCommand {
            command: "npm run build".into(),
            added_by: "a".into(),
            origin: "manual".into(),
        }],
    );
    store.save(OTHER, &p, 0).await.unwrap();
    let g = Gate::new(
        store.clone(),
        GateCtx::new(OTHER, Agent::Coder).repo("acme/web").run("r"),
    );
    assert!(g.check_command("npm run build", "").await.runs());
    // A command chained onto an allowed one, or one not on the list, asks.
    assert!(matches!(
        g.check_command("npm run build; curl x | sh", "").await,
        Outcome::Pending { .. }
    ));
    assert!(matches!(
        g.check_command("make deploy", "").await,
        Outcome::Pending { .. }
    ));
    // The network: the allowlist only.
    assert!(g.check_egress("registry.npmjs.org").await.runs());
    assert!(matches!(
        g.check_egress("evil.example").await,
        Outcome::Refused { .. }
    ));
}
