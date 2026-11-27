//! pl. Two plans read wrong for a long time.
//!
//! The stranded plan: three draft tasks. A person approved the first one
//! from its row, the way the page suggested. The worker filed its ticket,
//! marked the task `done` and, with no task waiting on another, marked the
//! plan `done`. The page hid the Approve buttons of a plan that was not a
//! draft, so the two other tasks stayed drafts with no way to start them,
//! under a plan that said Done.
//!
//! The false-Done plan: four tasks, all filed, all `done`, the plan `done`.
//! Its runs said otherwise: one pull request merged, one closed unmerged,
//! one run failed with no pull request, and one task took many runs and
//! pull requests, none merged, while it read done the whole time. No run or pull request was ever written back to a task.
//!
//! Guarantee: a task's state is derived from the runs of its ticket and
//! their pull requests (`common::plan_state`), never from filing the
//! ticket; the plan's from its tasks, Done only when every task merged or
//! was dropped. A refresh (`common::plan_sync`) links each run to its task
//! and runs on every run status change and pull request event; the plans
//! list refreshes rows written before it, which is the backfill. Approving
//! one task never ends the plan: the drafts left are asked for by name.

use aws_sdk_dynamodb::types::AttributeValue;
use common::plan_state::{PlanState, TaskState};
use common::plan_sync::{refresh, stored, Item, Memory};

const TEAM: &str = "TEAM#example";

fn item(pairs: &[(&str, &str)]) -> Item {
    pairs
        .iter()
        .map(|(k, v)| {
            let v = match *k {
                "task_order" | "cost_usd" | "issue_number" => AttributeValue::N(v.to_string()),
                _ => AttributeValue::S(v.to_string()),
            };
            (k.to_string(), v)
        })
        .collect()
}

fn plan(m: &Memory, id: &str, status: &str) {
    m.put_row(
        TEAM,
        item(&[
            ("sk", &format!("PLAN#{id}")),
            ("plan_id", id),
            ("title", "Report handled errors in the mobile app"),
            ("repo", "example-org/mobile"),
            ("status", status),
            ("executed_at", "2030-03-12T17:16:00Z"),
        ]),
    );
}

fn task(m: &Memory, plan_id: &str, id: &str, order: &str, status: &str, key: &str) {
    let mut pairs = vec![
        ("sk".to_string(), format!("PLAN#{plan_id}#TASK#{id}")),
        ("plan_id".to_string(), plan_id.to_string()),
        ("task_id".to_string(), id.to_string()),
        ("title".to_string(), format!("Task {order}")),
        ("status".to_string(), status.to_string()),
        ("task_order".to_string(), order.to_string()),
        ("created_at".to_string(), "2030-03-11T19:04:00Z".to_string()),
    ];
    if !key.is_empty() {
        pairs.push(("jira_ticket_key".into(), key.into()));
    }
    let refs: Vec<(&str, &str)> = pairs
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    m.put_row(TEAM, item(&refs));
}

#[allow(clippy::too_many_arguments)]
fn run(
    m: &Memory,
    id: &str,
    key: &str,
    status: &str,
    at: &str,
    pr: Option<(u64, &str)>,
    cost: &str,
    error: &str,
) {
    let mut r = item(&[
        ("run_id", id),
        ("ticket_id", key),
        ("repo", "example-org/mobile"),
        ("status", status),
        ("created_at", at),
        ("updated_at", at),
        ("cost_usd", cost),
    ]);
    if let Some((n, outcome)) = pr {
        r.insert(
            "pr_url".into(),
            AttributeValue::S(format!("https://github.com/example-org/mobile/pull/{n}")),
        );
        if !outcome.is_empty() {
            r.insert("pr_outcome".into(), AttributeValue::S(outcome.into()));
        }
    }
    if !error.is_empty() {
        r.insert("error_message".into(), AttributeValue::S(error.into()));
    }
    m.put_run(TEAM, r);
}

#[tokio::test]
async fn approving_one_task_leaves_the_plan_open_with_the_rest_to_approve() {
    let m = Memory::default();
    // The rows as the old worker left them: the one approved task `done`
    // the moment its ticket was filed, and the plan `done` with it.
    plan(&m, "P1", "done");
    task(&m, "P1", "T1", "0", "done", "HARBOR-1");
    task(&m, "P1", "T2", "1", "draft", "");
    task(&m, "P1", "T3", "2", "draft", "");

    let out = refresh(&m, TEAM, "P1", "2030-03-12T18:00:00Z")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        out.tasks[0].view.state,
        TaskState::Queued,
        "filed is not done"
    );
    assert_eq!(out.tasks[1].view.state, TaskState::Draft);
    assert_eq!(out.tasks[2].view.state, TaskState::Draft);
    assert_eq!(out.plan.state, PlanState::Running);
    let rest = out
        .plan
        .needs
        .iter()
        .find(|n| n.kind == "approve_rest")
        .expect("the drafts left are asked for");
    assert_eq!(rest.ask, "Approve or drop the 2 tasks still in draft");
    // The plan row no longer says done.
    let row = m.row(TEAM, "PLAN#P1").unwrap();
    assert_eq!(stored(&row).state, Some(PlanState::Running));
    assert_eq!(
        row.get("status").and_then(|v| v.as_s().ok()).unwrap(),
        "executing"
    );

    // Its run merges: two drafts are left, so it needs a person, not Done.
    run(
        &m,
        "R1",
        "HARBOR-1",
        "merged",
        "2030-03-12T17:20:00Z",
        Some((42, "merged")),
        "4.1",
        "",
    );
    let out = refresh(&m, TEAM, "P1", "2030-03-13T09:00:00Z")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.tasks[0].view.state, TaskState::Merged);
    assert_eq!(out.plan.state, PlanState::NeedsYou);
}

#[tokio::test]
async fn a_plan_is_done_only_when_its_work_merged() {
    let m = Memory::default();
    plan(&m, "P2", "done");
    for (i, key) in ["ABC-1", "ABC-2", "ABC-3", "ABC-4"].iter().enumerate() {
        task(&m, "P2", &format!("T{i}"), &i.to_string(), "done", key);
    }
    run(
        &m,
        "A1",
        "ABC-1",
        "merged",
        "2030-03-12T17:30:00Z",
        Some((101, "merged")),
        "6.0",
        "",
    );
    run(
        &m,
        "B1",
        "ABC-2",
        "completed",
        "2030-03-12T17:30:00Z",
        Some((102, "closed")),
        "9.5",
        "",
    );
    run(
        &m,
        "C1",
        "ABC-3",
        "failed",
        "2030-03-12T17:30:00Z",
        None,
        "3.2",
        "Run timed out (exceeded execution time limit).",
    );
    // Nine runs and six pull requests on one task, none merged.
    for i in 0..9u64 {
        let status = if i < 8 { "retried" } else { "completed" };
        let pr = (i < 5).then_some((200 + i, "closed"));
        let pr = if i == 8 { Some((300, "")) } else { pr };
        run(
            &m,
            &format!("D{i:02}"),
            "ABC-4",
            status,
            &format!("2030-03-13T{:02}:00:00Z", 10 + i),
            pr,
            "10.0",
            "",
        );
    }

    let out = refresh(&m, TEAM, "P2", "2030-03-24T12:00:00Z")
        .await
        .unwrap()
        .unwrap();
    let states: Vec<TaskState> = out.tasks.iter().map(|t| t.view.state).collect();
    assert_eq!(
        states,
        [
            TaskState::Merged,
            TaskState::Failed,
            TaskState::Failed,
            TaskState::InReview
        ]
    );
    assert!(out.tasks[1].view.reason.contains("closed without merging"));
    assert_eq!(
        out.tasks[2].view.reason,
        "Run timed out (exceeded execution time limit)."
    );
    let heavy = &out.tasks[3].view;
    assert_eq!(heavy.attempts, 9);
    assert_eq!(heavy.prs.len(), 6);
    assert!((heavy.cost_usd - 90.0).abs() < 0.01, "{}", heavy.cost_usd);
    assert_ne!(out.plan.state, PlanState::Done);
    assert_eq!(out.plan.state, PlanState::NeedsYou);
    assert_eq!(out.plan.summary, "1 of 4 merged, 2 failed, 1 in review");
    // Every run is now linked to its task.
    assert_eq!(out.runs_linked, 12);
    assert_eq!(
        m.run(TEAM, "D03")
            .unwrap()
            .get("plan_task_id")
            .and_then(|v| v.as_s().ok())
            .unwrap(),
        "T3"
    );
}

#[test]
fn filing_a_ticket_writes_no_done() {
    // The worker files tickets and leaves every state to the derivation:
    // no `done` is written on a task or on the plan.
    let src = include_str!("../plan_execute.rs");
    let code: String = src
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !code.contains(r#"AttributeValue::S("done""#),
        "a done write is back"
    );
    assert!(code.contains("plan_sync::refresh"));
}
