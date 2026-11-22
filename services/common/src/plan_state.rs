//! Where a plan and each of its tasks stand, derived from the work itself.
//!
//! A plan's task used to be marked `done` the moment its ticket was filed,
//! and the plan `done` once no task waited on another. Nothing then wrote
//! the runs or pull requests back to the task, so a plan read "Done" while
//! most of its pull requests were still open, closed unmerged, or burning
//! attempt after attempt. Approving one task at a time ended the plan with
//! the other tasks still drafts and no way to approve them.
//!
//! The state of a task is now derived, every time, from the task row and
//! the runs of its ticket ([`task_view`]); the plan's from its tasks
//! ([`plan_view`]). Filing the ticket is a fact on the task, never a state.
//!
//! Task states, a small fixed set:
//!
//! | State | When |
//! | --- | --- |
//! | Draft | Not approved yet. |
//! | Queued | Approved, waiting: on the task it runs after, on its ticket, or on its run to start. |
//! | Working | Its newest run is working. |
//! | Needs you | Its newest run asks a person something (a question, a review, an approval, checks it can't fix). |
//! | Failed | Its newest run failed with no pull request left open, a person cancelled it, or its pull request closed unmerged. The reason is kept. |
//! | In review | A pull request is open and the next step is not a person's. |
//! | Merged | A pull request of the task merged. |
//! | Dropped | A person dropped the task, or closed its run on purpose. |
//!
//! Plan states: Drafting (nothing approved yet), Running, Needs you (a task
//! needs a person, failed, or drafts remain once the rest finished), Done
//! (every task Merged or Dropped), Stalled (work open and nothing moved for
//! [`STALE_HOURS`]).
//!
//! Everything here is pure: the store reads and writes are in
//! [`crate::plan_sync`].

use serde::{Deserialize, Serialize};

use crate::merge_standing::Standing;
use crate::run_wait::WaitingOn;
use crate::trace::{State as ProofState, Trace};

/// A plan with open work and no activity for this long reads Stalled.
pub const STALE_HOURS: i64 = 72;

/// Where a task stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    #[default]
    Draft,
    Queued,
    Working,
    NeedsYou,
    Failed,
    InReview,
    Merged,
    Dropped,
}

impl TaskState {
    pub const ALL: [TaskState; 8] = [
        TaskState::Draft,
        TaskState::Queued,
        TaskState::Working,
        TaskState::NeedsYou,
        TaskState::Failed,
        TaskState::InReview,
        TaskState::Merged,
        TaskState::Dropped,
    ];

    /// The stable code the API and the store use.
    pub fn code(self) -> &'static str {
        match self {
            TaskState::Draft => "draft",
            TaskState::Queued => "queued",
            TaskState::Working => "working",
            TaskState::NeedsYou => "needs_you",
            TaskState::Failed => "failed",
            TaskState::InReview => "in_review",
            TaskState::Merged => "merged",
            TaskState::Dropped => "dropped",
        }
    }

    pub fn from_code(code: &str) -> Option<TaskState> {
        TaskState::ALL.into_iter().find(|s| s.code() == code)
    }

    /// The words people see.
    pub fn label(self) -> &'static str {
        match self {
            TaskState::Draft => "Draft",
            TaskState::Queued => "Queued",
            TaskState::Working => "Working",
            TaskState::NeedsYou => "Needs you",
            TaskState::Failed => "Failed",
            TaskState::InReview => "In review",
            TaskState::Merged => "Merged",
            TaskState::Dropped => "Dropped",
        }
    }

    /// Nothing more happens on the task.
    pub fn is_finished(self) -> bool {
        matches!(self, TaskState::Merged | TaskState::Dropped)
    }

    /// Approved and not finished: work is open on it.
    pub fn is_open(self) -> bool {
        !matches!(
            self,
            TaskState::Draft | TaskState::Merged | TaskState::Dropped
        )
    }
}

/// Where a plan stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PlanState {
    #[default]
    Drafting,
    Running,
    NeedsYou,
    Done,
    Stalled,
}

impl PlanState {
    pub fn code(self) -> &'static str {
        match self {
            PlanState::Drafting => "drafting",
            PlanState::Running => "running",
            PlanState::NeedsYou => "needs_you",
            PlanState::Done => "done",
            PlanState::Stalled => "stalled",
        }
    }

    pub fn from_code(code: &str) -> Option<PlanState> {
        [
            PlanState::Drafting,
            PlanState::Running,
            PlanState::NeedsYou,
            PlanState::Done,
            PlanState::Stalled,
        ]
        .into_iter()
        .find(|s| s.code() == code)
    }

    pub fn label(self) -> &'static str {
        match self {
            PlanState::Drafting => "Drafting",
            PlanState::Running => "Running",
            PlanState::NeedsYou => "Needs you",
            PlanState::Done => "Done",
            PlanState::Stalled => "Stalled",
        }
    }

    /// The plan's older `status` word, kept for clients that read it.
    pub fn legacy_status(self) -> &'static str {
        match self {
            PlanState::Drafting => "draft",
            PlanState::Done => "done",
            _ => "executing",
        }
    }
}

/// A task row, as far as its state goes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskFacts {
    pub task_id: String,
    pub title: String,
    pub description: String,
    pub acceptance_criteria: String,
    /// The row's own word: `draft`, `approved`, `rejected`, `queued`,
    /// `waiting`, `running` (filing its ticket), `filed` (older rows say
    /// `done` or `issue_created`), `failed`.
    pub status: String,
    pub depends_on: String,
    /// Why filing failed, when it did.
    pub failure_reason: String,
    pub created_at: String,
    pub approved_at: String,
    pub rejected_at: String,
    pub rejected_by: String,
    /// The last edit, when one was made after the task was written.
    pub edited_at: String,
    /// `person` or `agent`.
    pub edited_by_kind: String,
    pub edited_by: String,
    /// Who wrote the task: `agent` (the plan chat) or `person`.
    pub added_by_kind: String,
    /// Criteria ids (`C1`) the task says it covers.
    pub covers: Vec<String>,
}

/// One pull request of a run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrFacts {
    pub repo: String,
    pub number: u64,
    pub url: String,
    /// `merged`, `closed`, or empty while open.
    pub outcome: String,
}

/// A run of the task's ticket, as far as the task's state goes.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunFacts {
    pub run_id: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    pub cost_usd: f64,
    pub current_pass: String,
    pub error: String,
    pub close_reason: String,
    pub waiting_on: Option<WaitingOn>,
    pub prs: Vec<PrFacts>,
    /// The record's own `pr_outcome` (its primary pull request).
    pub pr_outcome: String,
    /// `passed`, `failed`, or empty.
    pub ci_last: String,
    pub standing: Option<Standing>,
    /// The latest review a person gave on GitHub: `approved`,
    /// `changes_requested`, or empty.
    pub review: String,
    pub additions: Option<u64>,
    pub deletions: Option<u64>,
    pub trace: Option<Trace>,
    /// The files the run's branch changed.
    pub files: Vec<String>,
    /// Scope ledger items a round moved into scope: `(item, why)`.
    pub scope_moved_in: Vec<(String, String)>,
}

impl RunFacts {
    fn merged(&self) -> bool {
        self.status == "merged"
            || self.pr_outcome == "merged"
            || self.prs.iter().any(|p| p.outcome == "merged")
    }

    fn open_pr(&self) -> Option<&PrFacts> {
        if self.pr_outcome == "merged" || self.pr_outcome == "closed" {
            return None;
        }
        self.prs.iter().find(|p| p.outcome.is_empty())
    }

    fn closed_pr(&self) -> Option<&PrFacts> {
        if self.pr_outcome == "closed" {
            return self.prs.first();
        }
        self.prs.iter().find(|p| p.outcome == "closed")
    }
}

/// A pull request as a chip: its number, coloured by checks and review.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrChip {
    pub number: u64,
    pub url: String,
    pub repo: String,
    /// `open`, `draft`, `merged` or `closed`.
    pub state: String,
    /// `passed`, `failed`, `running` or `unknown`.
    pub checks: String,
    /// `approved`, `changes_requested`, `waiting` or `none`.
    pub review: String,
    /// The colour: `ready` (green), `waiting` (yellow), `failing` (red),
    /// `merged` (purple), `closed` (grey).
    pub tone: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additions: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletions: Option<u64>,
    /// The run that opened it.
    pub run_id: String,
}

/// One run of a task, as listed on it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunLine {
    pub run_id: String,
    pub status: String,
    pub created_at: String,
    pub cost_usd: f64,
}

/// Something a person has to do for the plan to go on: the exact ask.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NeedsItem {
    pub task_id: String,
    /// `answer`, `review`, `approval`, `checks`, `merge`, `failed`,
    /// `closed_pr` or `approve_rest`.
    pub kind: String,
    /// What to do, one line.
    pub ask: String,
    /// Why, or what is open.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
    /// Where to do it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    /// The button: `answer` (the run page), `open_pr`, `open_link`,
    /// `retry` or `approve`.
    pub action: String,
}

/// A change to the plan after it was approved.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drift {
    /// `added`, `changed`, `dropped` or `scope`.
    pub kind: String,
    pub detail: String,
    /// `agent`, `person`, or empty when not known.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub by_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub at: String,
}

/// A task with where it stands.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskView {
    pub state: TaskState,
    /// One line: what it is doing, what it waits on, or what blocks it.
    pub detail: String,
    /// Why it failed (Failed only).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// Every attempt, oldest first.
    pub runs: Vec<RunLine>,
    pub prs: Vec<PrChip>,
    pub attempts: usize,
    pub cost_usd: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub needs: Option<NeedsItem>,
    pub drift: Vec<Drift>,
    /// The ticket was filed (a fact, not a state).
    pub ticket_filed: bool,
    /// The newest time anything happened on the task.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_activity: String,
}

/// The runs that count, oldest first. Archived runs are hidden from people
/// and say nothing about the work.
fn ordered(runs: &[RunFacts]) -> Vec<&RunFacts> {
    let mut v: Vec<&RunFacts> = runs.iter().filter(|r| r.status != "archived").collect();
    v.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then(a.run_id.cmp(&b.run_id))
    });
    v
}

fn pass_words(pass: &str) -> String {
    match pass.trim() {
        "" => "working on it".into(),
        "triage" => "reading the ticket".into(),
        "plan" => "planning the change".into(),
        "implement" => "writing the change".into(),
        "test" => "running the tests".into(),
        "security" => "checking security".into(),
        "review" => "reviewing its own work".into(),
        "feedback" => "taking review feedback".into(),
        "pr" => "opening the pull request".into(),
        other => other.replace('_', " "),
    }
}

fn one_line(s: &str, max: usize) -> String {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    crate::truncate_str(&s, max).to_string()
}

/// The chip for one pull request of a run.
pub fn chip(run: &RunFacts, pr: &PrFacts) -> PrChip {
    let merged = pr.outcome == "merged"
        || (run.prs.len() <= 1 && (run.pr_outcome == "merged" || run.status == "merged"));
    let closed =
        !merged && (pr.outcome == "closed" || (run.prs.len() <= 1 && run.pr_outcome == "closed"));
    let code = run.standing.as_ref().map(|s| s.code.as_str()).unwrap_or("");
    let draft = code == "draft";
    let checks = if code == "checks_failing" || (code.is_empty() && run.ci_last == "failed") {
        "failed"
    } else if code == "checks_running" || run.status == "awaiting_ci" {
        "running"
    } else if run.ci_last == "passed"
        || matches!(
            code,
            "auto_merge_off" | "needs_person_approval" | "repo_requires_person_review" | "merging"
        )
    {
        "passed"
    } else if run.ci_last == "failed" {
        "failed"
    } else {
        "unknown"
    };
    let review = match (run.review.as_str(), code) {
        ("approved", _) | (_, "merging") => "approved",
        ("changes_requested", _) | (_, "review_changes_requested") => "changes_requested",
        (_, "needs_person_approval" | "repo_requires_person_review" | "reviewing") => "waiting",
        _ => "none",
    };
    let state = if merged {
        "merged"
    } else if closed {
        "closed"
    } else if draft {
        "draft"
    } else {
        "open"
    };
    let tone = match state {
        "merged" => "merged",
        "closed" => "closed",
        _ if checks == "failed" || review == "changes_requested" => "failing",
        _ if checks == "passed"
            && !draft
            && matches!(code, "merging" | "auto_merge_off" | "")
            && review != "waiting" =>
        {
            "ready"
        }
        _ => "waiting",
    };
    PrChip {
        number: pr.number,
        url: pr.url.clone(),
        repo: pr.repo.clone(),
        state: state.into(),
        checks: checks.into(),
        review: review.into(),
        tone: tone.into(),
        additions: run.additions,
        deletions: run.deletions,
        run_id: run.run_id.clone(),
    }
}

/// Where a task stands, from its row and the runs of its ticket.
/// `waits_on` is the title of the task it runs after, when it has one.
pub fn task_view(task: &TaskFacts, runs: &[RunFacts], waits_on: Option<&str>) -> TaskView {
    let runs = ordered(runs);
    let mut v = TaskView {
        runs: runs
            .iter()
            .map(|r| RunLine {
                run_id: r.run_id.clone(),
                status: r.status.clone(),
                created_at: r.created_at.clone(),
                cost_usd: r.cost_usd,
            })
            .collect(),
        attempts: runs.len(),
        cost_usd: runs.iter().map(|r| r.cost_usd).sum(),
        ticket_filed: !runs.is_empty()
            || matches!(task.status.as_str(), "filed" | "done" | "issue_created"),
        ..Default::default()
    };
    // Every pull request any attempt opened, newest attempt first.
    let mut seen = std::collections::HashSet::new();
    for r in runs.iter().rev() {
        for pr in &r.prs {
            if seen.insert((pr.repo.to_ascii_lowercase(), pr.number)) {
                v.prs.push(chip(r, pr));
            }
        }
    }
    v.last_activity = runs
        .iter()
        .map(|r| {
            if r.updated_at.is_empty() {
                r.created_at.clone()
            } else {
                r.updated_at.clone()
            }
        })
        .chain([
            task.approved_at.clone(),
            task.rejected_at.clone(),
            task.edited_at.clone(),
        ])
        .max()
        .unwrap_or_default();

    let set = |v: &mut TaskView, state: TaskState, detail: String| {
        v.state = state;
        v.detail = detail;
    };

    // A person dropped it.
    if task.status == "rejected" {
        let by = if task.rejected_by.is_empty() {
            String::new()
        } else {
            format!(" by {}", task.rejected_by)
        };
        set(&mut v, TaskState::Dropped, format!("Dropped{by}"));
        return v;
    }
    // Any attempt's pull request merged: the work is in.
    if let Some(r) = runs.iter().rev().find(|r| r.merged()) {
        let pr = r
            .prs
            .iter()
            .find(|p| p.outcome == "merged")
            .or(r.prs.first());
        let what = pr
            .map(|p| format!("Pull request #{} merged", p.number))
            .unwrap_or_else(|| "Merged".into());
        set(&mut v, TaskState::Merged, what);
        return v;
    }
    let Some(r) = runs.last().copied() else {
        let (state, detail) = match task.status.as_str() {
            "draft" | "" => (TaskState::Draft, "Waiting for approval".to_string()),
            "failed" => {
                v.reason = if task.failure_reason.trim().is_empty() {
                    "The ticket could not be filed.".into()
                } else {
                    one_line(&task.failure_reason, 400)
                };
                v.needs = Some(NeedsItem {
                    task_id: task.task_id.clone(),
                    kind: "failed".into(),
                    ask: format!("Task \"{}\" failed", one_line(&task.title, 120)),
                    detail: v.reason.clone(),
                    action: "retry".into(),
                    ..Default::default()
                });
                (TaskState::Failed, v.reason.clone())
            }
            "waiting" => (
                TaskState::Queued,
                match waits_on {
                    Some(t) => format!("Waits for \"{}\" to merge", one_line(t, 120)),
                    None => "Waits for the task it runs after to merge".into(),
                },
            ),
            "filed" | "done" | "issue_created" => (
                TaskState::Queued,
                "Ticket filed; waiting for its run to start".into(),
            ),
            "running" => (TaskState::Queued, "Filing its ticket".into()),
            _ => (TaskState::Queued, "Waiting to file its ticket".into()),
        };
        set(&mut v, state, detail);
        return v;
    };

    let open = r.open_pr().cloned();
    let error = one_line(&r.error, 400);
    let needs = |kind: &str, ask: String, detail: String, url: String, action: &str| NeedsItem {
        task_id: task.task_id.clone(),
        kind: kind.into(),
        ask,
        detail,
        url,
        run_id: r.run_id.clone(),
        action: action.into(),
    };
    match r.status.as_str() {
        "pending" | "queued" => set(&mut v, TaskState::Queued, "Its run is queued".into()),
        "retried" => set(
            &mut v,
            TaskState::Queued,
            "A new attempt is starting".into(),
        ),
        "running" | "awaiting_setup" => {
            let doing = pass_words(&r.current_pass);
            set(&mut v, TaskState::Working, format!("Working: {doing}"));
        }
        "awaiting_ci" => match &open {
            Some(pr) => set(
                &mut v,
                TaskState::InReview,
                format!("Checks are running on pull request #{}", pr.number),
            ),
            None => set(&mut v, TaskState::Working, "Waiting on CI".into()),
        },
        "needs_input" | "needs_outside_change" => {
            let (ask, detail, url, kind) = match &r.waiting_on {
                Some(w) => {
                    let kind = if w.questions > 0 {
                        "answer"
                    } else if w.action.to_ascii_lowercase().starts_with("review") {
                        "review"
                    } else {
                        "approval"
                    };
                    (w.action.clone(), w.detail.clone(), w.url.clone(), kind)
                }
                None => (
                    "Answer what the run asks on its page".into(),
                    error.clone(),
                    String::new(),
                    "answer",
                ),
            };
            let action = if kind == "review" && !url.is_empty() {
                "open_pr"
            } else {
                "answer"
            };
            set(&mut v, TaskState::NeedsYou, one_line(&ask, 200));
            v.needs = Some(needs(kind, ask, detail, url, action));
        }
        "failed" => match &open {
            Some(pr) if r.ci_last == "failed" => {
                let ask = format!(
                    "Checks fail on pull request #{} and CoderHelm could not fix them",
                    pr.number
                );
                set(&mut v, TaskState::NeedsYou, ask.clone());
                v.needs = Some(needs("checks", ask, error, pr.url.clone(), "open_pr"));
            }
            Some(pr) => {
                let ask = format!(
                    "The run stopped with pull request #{} open; finish or close it",
                    pr.number
                );
                set(&mut v, TaskState::NeedsYou, ask.clone());
                v.needs = Some(needs("review", ask, error, pr.url.clone(), "open_pr"));
            }
            None => {
                v.reason = if error.is_empty() {
                    "The run failed and gave no reason.".into()
                } else {
                    error.clone()
                };
                let why = v.reason.clone();
                set(&mut v, TaskState::Failed, why);
                v.needs = Some(needs(
                    "failed",
                    format!("Task \"{}\" failed", one_line(&task.title, 120)),
                    v.reason.clone(),
                    String::new(),
                    "retry",
                ));
            }
        },
        "cancelled" => {
            v.reason = "A person cancelled its run.".into();
            let why = v.reason.clone();
            set(&mut v, TaskState::Failed, why);
            v.needs = Some(needs(
                "failed",
                format!("Task \"{}\" was cancelled", one_line(&task.title, 120)),
                "Retry it, or drop the task.".into(),
                String::new(),
                "retry",
            ));
        }
        "closed" => {
            let why = one_line(&r.close_reason, 200);
            let detail = if why.is_empty() {
                "A person closed its run".to_string()
            } else {
                format!("A person closed its run: {why}")
            };
            set(&mut v, TaskState::Dropped, detail);
        }
        "covered_by_pr" => {
            let url = open.as_ref().map(|p| p.url.clone()).unwrap_or_default();
            let ask = "An open pull request already does this work; merge it or drop the task"
                .to_string();
            set(&mut v, TaskState::NeedsYou, ask.clone());
            v.needs = Some(needs("review", ask, error, url, "open_pr"));
        }
        // Finished its work: the pull request decides.
        "completed" | "success" => match (&open, r.closed_pr()) {
            (Some(pr), _) => match r.standing.as_ref().filter(|s| s.needs_person) {
                Some(s) => {
                    let kind = if s.code == "auto_merge_off" {
                        "merge"
                    } else {
                        "approval"
                    };
                    let ask = if kind == "merge" {
                        format!("Merge pull request #{}", pr.number)
                    } else {
                        format!("Approve pull request #{}", pr.number)
                    };
                    set(&mut v, TaskState::NeedsYou, ask.clone());
                    v.needs = Some(needs(kind, ask, s.text.clone(), pr.url.clone(), "open_pr"));
                }
                None => {
                    let detail = r
                        .standing
                        .as_ref()
                        .map(|s| s.text.clone())
                        .unwrap_or_else(|| {
                            format!("Pull request #{} is open for review", pr.number)
                        });
                    set(&mut v, TaskState::InReview, detail);
                }
            },
            (None, Some(pr)) => {
                v.reason = format!("Pull request #{} was closed without merging.", pr.number);
                let why = v.reason.clone();
                set(&mut v, TaskState::Failed, why);
                v.needs = Some(needs(
                    "closed_pr",
                    format!("Pull request #{} closed unmerged", pr.number),
                    "Retry the task, or drop it.".into(),
                    pr.url.clone(),
                    "retry",
                ));
            }
            (None, None) => {
                v.reason = if error.is_empty() {
                    "The run finished without a pull request.".into()
                } else {
                    format!("The run finished without a pull request: {error}")
                };
                let why = v.reason.clone();
                set(&mut v, TaskState::Failed, why);
                v.needs = Some(needs(
                    "failed",
                    format!(
                        "Task \"{}\" has no pull request",
                        one_line(&task.title, 120)
                    ),
                    v.reason.clone(),
                    String::new(),
                    "retry",
                ));
            }
        },
        other => set(
            &mut v,
            TaskState::Working,
            format!("Working ({})", other.replace('_', " ")),
        ),
    }
    v
}

/// Changes to the task after its plan was approved, and work outside its
/// written scope. `started_at` is when the plan first ran ("" before).
pub fn task_drift(
    task: &TaskFacts,
    view: &TaskView,
    runs: &[RunFacts],
    started_at: &str,
) -> Vec<Drift> {
    let mut out = Vec::new();
    if !started_at.is_empty() {
        let after = |t: &str| !t.is_empty() && t > started_at;
        if after(&task.created_at) {
            out.push(Drift {
                kind: "added".into(),
                detail: "Added after the plan was approved".into(),
                by: task.added_by_kind.clone(),
                at: task.created_at.clone(),
                ..Default::default()
            });
        }
        if after(&task.edited_at) {
            out.push(Drift {
                kind: "changed".into(),
                detail: "Changed after the plan was approved".into(),
                by: task.edited_by_kind.clone(),
                by_name: task.edited_by.clone(),
                at: task.edited_at.clone(),
            });
        }
        if task.status == "rejected" && after(&task.rejected_at) {
            out.push(Drift {
                kind: "dropped".into(),
                detail: "Dropped after the plan was approved".into(),
                by: "person".into(),
                by_name: task.rejected_by.clone(),
                at: task.rejected_at.clone(),
            });
        }
    }
    // Scope: the newest attempt that opened a pull request.
    let Some(run) = ordered(runs).into_iter().rev().find(|r| !r.prs.is_empty()) else {
        return out;
    };
    let _ = view;
    let outside = outside_scope(
        &format!("{}\n{}", task.description, task.acceptance_criteria),
        &run.files,
    );
    if !outside.is_empty() {
        let shown: Vec<&str> = outside.iter().take(3).map(String::as_str).collect();
        let more = outside.len().saturating_sub(shown.len());
        let mut detail = format!(
            "The pull request changes files outside the task's planned scope: {}",
            shown.join(", ")
        );
        if more > 0 {
            detail.push_str(&format!(" and {more} more"));
        }
        out.push(Drift {
            kind: "scope".into(),
            detail,
            by: "agent".into(),
            ..Default::default()
        });
    }
    for (item, why) in &run.scope_moved_in {
        out.push(Drift {
            kind: "scope".into(),
            detail: format!(
                "Scope widened during the run: {} ({})",
                one_line(item, 120),
                one_line(why, 160)
            ),
            by: "agent".into(),
            ..Default::default()
        });
    }
    out
}

/// The changed files outside what the task's text names. The task names
/// its scope by paths (`marks_of`, the scope ledger's reading of a text);
/// a file is inside when it is one of them or under one's folder. Tests,
/// docs and lockfiles go with any change. A task that names no path has no
/// written scope to be outside of.
pub fn outside_scope(task_text: &str, files: &[String]) -> Vec<String> {
    let paths: Vec<String> = crate::scope_ledger::marks_of(task_text)
        .into_iter()
        .filter(|m| m.contains('/') || m.contains('.'))
        .map(|m| {
            m.trim_start_matches("./")
                .trim_start_matches('/')
                .to_string()
        })
        .filter(|m| m.contains('/'))
        .collect();
    if paths.is_empty() {
        return Vec::new();
    }
    let roots: Vec<String> = paths
        .iter()
        .map(|p| {
            let last = p.rsplit('/').next().unwrap_or("");
            if last.contains('.') {
                p.rsplit_once('/')
                    .map(|(d, _)| d.to_string())
                    .unwrap_or_default()
            } else {
                p.trim_end_matches('/').to_string()
            }
        })
        .filter(|r| !r.is_empty())
        .collect();
    files
        .iter()
        .filter(|f| {
            let f = f.trim_start_matches("./");
            let lower = f.to_ascii_lowercase();
            let goes_along = crate::trace::is_test_path(f)
                || lower.ends_with(".md")
                || lower.ends_with(".lock")
                || lower.ends_with("package-lock.json")
                || lower.ends_with("pnpm-lock.yaml");
            !goes_along
                && !paths.iter().any(|p| f == p)
                && !roots
                    .iter()
                    .any(|r| f.starts_with(&format!("{r}/")) || f == r)
        })
        .cloned()
        .collect()
}

/// One success criterion of a plan with the evidence the work gave it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Criterion {
    /// `C1`, `C2`, ...
    pub id: String,
    pub text: String,
    /// The tasks that cover it.
    pub tasks: Vec<String>,
    pub evidence: Vec<Evidence>,
    /// Proven by the work.
    pub met: bool,
}

/// What the work shows for a criterion (the run's criteria trace).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    pub task_id: String,
    pub run_id: String,
    /// The trace's proof state: `proven`, `not_the_ticket`, `failing`,
    /// `missing`, `unverified` or `not_run`.
    pub state: String,
    /// The words for it.
    pub words: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub by: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tests: Vec<String>,
}

/// The bullet lines of a criteria text, each one criterion.
pub fn bullets(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        let l = line.trim();
        let item = l
            .strip_prefix("- [ ] ")
            .or_else(|| l.strip_prefix("- [x] "))
            .or_else(|| l.strip_prefix("- "))
            .or_else(|| l.strip_prefix("* "))
            .or_else(|| {
                let (n, rest) = l.split_once(". ")?;
                n.chars().all(|c| c.is_ascii_digit()).then_some(rest)
            });
        if let Some(item) = item.map(str::trim).filter(|s| !s.is_empty()) {
            out.push(item.to_string());
        }
    }
    if out.is_empty() && !text.trim().is_empty() {
        out.push(one_line(text, 400));
    }
    out
}

fn words_of(s: &str) -> Vec<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() > 2)
        .map(|w| w.to_lowercase())
        .collect()
}

/// Whether a trace row's criterion is this criterion: the same text, one
/// inside the other, or most of the shorter one's words shared.
pub fn same_criterion(a: &str, b: &str) -> bool {
    let (na, nb) = (words_of(a).join(" "), words_of(b).join(" "));
    if na.is_empty() || nb.is_empty() {
        return false;
    }
    if na == nb || na.contains(&nb) || nb.contains(&na) {
        return true;
    }
    let (wa, wb) = (words_of(a), words_of(b));
    let (short, long) = if wa.len() <= wb.len() {
        (&wa, &wb)
    } else {
        (&wb, &wa)
    };
    let shared = short.iter().filter(|w| long.contains(w)).count();
    short.len() >= 3 && shared * 10 >= short.len() * 7
}

fn proof_code(s: ProofState) -> &'static str {
    match s {
        ProofState::Proven => "proven",
        ProofState::NotTheTicket => "not_the_ticket",
        ProofState::Failing => "failing",
        ProofState::Missing => "missing",
        ProofState::Unverified => "unverified",
        ProofState::NotRun => "not_run",
    }
}

/// The plan's success criteria with their evidence. The plan's own list
/// when it has one (each task names what it covers, else it covers what its
/// acceptance criteria match); otherwise each task's acceptance criteria.
/// `tasks` pairs each task with the newest trace of its runs.
pub fn criteria(
    plan_criteria: &[String],
    tasks: &[(&TaskFacts, Option<&RunTrace>)],
) -> Vec<Criterion> {
    let mut out: Vec<Criterion> = Vec::new();
    if plan_criteria.iter().any(|c| !c.trim().is_empty()) {
        for (i, text) in plan_criteria
            .iter()
            .filter(|c| !c.trim().is_empty())
            .enumerate()
        {
            let id = format!("C{}", i + 1);
            let covering: Vec<&TaskFacts> = tasks
                .iter()
                .map(|(t, _)| *t)
                .filter(|t| {
                    if !t.covers.is_empty() {
                        t.covers.iter().any(|c| c.eq_ignore_ascii_case(&id))
                    } else {
                        bullets(&t.acceptance_criteria)
                            .iter()
                            .any(|b| same_criterion(b, text))
                    }
                })
                .collect();
            out.push(Criterion {
                id,
                text: text.trim().to_string(),
                tasks: covering.iter().map(|t| t.task_id.clone()).collect(),
                ..Default::default()
            });
        }
    } else {
        for (t, _) in tasks {
            if t.status == "rejected" {
                continue;
            }
            for b in bullets(&t.acceptance_criteria) {
                out.push(Criterion {
                    id: format!("C{}", out.len() + 1),
                    text: b,
                    tasks: vec![t.task_id.clone()],
                    ..Default::default()
                });
            }
        }
    }
    for c in out.iter_mut() {
        for (t, trace) in tasks {
            if !c.tasks.contains(&t.task_id) {
                continue;
            }
            let Some(rt) = trace else { continue };
            for row in rt
                .trace
                .rows
                .iter()
                .filter(|r| same_criterion(&r.criterion, &c.text))
            {
                c.evidence.push(Evidence {
                    task_id: t.task_id.clone(),
                    run_id: rt.run_id.clone(),
                    state: proof_code(row.proof.state).into(),
                    words: row.proof.state.words().into(),
                    by: row.proof.by.clone(),
                    tests: row.tests.iter().take(4).cloned().collect(),
                });
            }
        }
        c.met = !c.evidence.is_empty() && c.evidence.iter().all(|e| e.state == "proven");
    }
    out
}

/// A run's criteria trace with the run it came from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunTrace {
    pub run_id: String,
    pub trace: Trace,
}

/// The newest trace among a task's runs.
pub fn newest_trace(runs: &[RunFacts]) -> Option<RunTrace> {
    ordered(runs).into_iter().rev().find_map(|r| {
        r.trace.as_ref().map(|t| RunTrace {
            run_id: r.run_id.clone(),
            trace: t.clone(),
        })
    })
}

/// How many tasks are in each state, and what the plan spent.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Counts {
    pub draft: usize,
    pub queued: usize,
    pub working: usize,
    pub needs_you: usize,
    pub failed: usize,
    pub in_review: usize,
    pub merged: usize,
    pub dropped: usize,
}

impl Counts {
    pub fn add(&mut self, s: TaskState) {
        *self.slot(s) += 1;
    }

    fn slot(&mut self, s: TaskState) -> &mut usize {
        match s {
            TaskState::Draft => &mut self.draft,
            TaskState::Queued => &mut self.queued,
            TaskState::Working => &mut self.working,
            TaskState::NeedsYou => &mut self.needs_you,
            TaskState::Failed => &mut self.failed,
            TaskState::InReview => &mut self.in_review,
            TaskState::Merged => &mut self.merged,
            TaskState::Dropped => &mut self.dropped,
        }
    }

    pub fn get(&self, s: TaskState) -> usize {
        match s {
            TaskState::Draft => self.draft,
            TaskState::Queued => self.queued,
            TaskState::Working => self.working,
            TaskState::NeedsYou => self.needs_you,
            TaskState::Failed => self.failed,
            TaskState::InReview => self.in_review,
            TaskState::Merged => self.merged,
            TaskState::Dropped => self.dropped,
        }
    }

    pub fn total(&self) -> usize {
        TaskState::ALL.iter().map(|s| self.get(*s)).sum()
    }
}

/// A plan with where it stands.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PlanView {
    pub state: PlanState,
    /// One line: the count line people read first.
    pub summary: String,
    pub counts: Counts,
    pub cost_usd: f64,
    pub attempts: usize,
    /// Everything waiting on a person, in task order.
    pub needs: Vec<NeedsItem>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_activity: String,
}

/// Where the plan stands, from its tasks' views (in task order). `now` and
/// the views' activity times are RFC 3339.
pub fn plan_view(tasks: &[(&TaskFacts, &TaskView)], now: &str) -> PlanView {
    let mut p = PlanView::default();
    for (_, v) in tasks {
        p.counts.add(v.state);
        p.cost_usd += v.cost_usd;
        p.attempts += v.attempts;
        if let Some(n) = &v.needs {
            p.needs.push(n.clone());
        }
        if v.last_activity > p.last_activity {
            p.last_activity = v.last_activity.clone();
        }
    }
    let c = &p.counts;
    let total = c.total();
    let started = total - c.draft - c.dropped > 0 || c.merged > 0;
    let open = c.queued + c.working + c.in_review;
    p.state = if total == 0 || !started {
        PlanState::Drafting
    } else if c.merged + c.dropped == total {
        PlanState::Done
    } else if c.needs_you + c.failed > 0 || (open == 0 && c.draft > 0) {
        // A task asks something or failed, or only drafts are left.
        PlanState::NeedsYou
    } else if open > 0 && stale(&p.last_activity, now) {
        PlanState::Stalled
    } else {
        PlanState::Running
    };
    // Drafts left once the plan started: they are approved from here.
    if started && c.draft > 0 {
        p.needs.push(NeedsItem {
            kind: "approve_rest".into(),
            ask: if c.draft == 1 {
                "Approve or drop the 1 task still in draft".into()
            } else {
                format!("Approve or drop the {} tasks still in draft", c.draft)
            },
            action: "approve".into(),
            ..Default::default()
        });
    }
    p.summary = summary(c);
    p
}

fn stale(last: &str, now: &str) -> bool {
    let (Ok(last), Ok(now)) = (
        chrono::DateTime::parse_from_rfc3339(last),
        chrono::DateTime::parse_from_rfc3339(now),
    ) else {
        return false;
    };
    (now - last).num_hours() >= STALE_HOURS
}

/// "3 of 7 merged, 2 in review, 1 needs you": merged first, then each state
/// with tasks in it, in the order people act on them.
pub fn summary(c: &Counts) -> String {
    let total = c.total();
    if total == 0 {
        return "No tasks".into();
    }
    // Nothing started yet: what is left to review.
    if c.draft + c.dropped == total && c.draft > 0 {
        let mut s = format!(
            "{} task{} to review",
            c.draft,
            if c.draft == 1 { "" } else { "s" }
        );
        if c.dropped > 0 {
            s.push_str(&format!(", {} dropped", c.dropped));
        }
        return s;
    }
    let mut parts = vec![format!("{} of {total} merged", c.merged)];
    for s in [
        TaskState::NeedsYou,
        TaskState::Failed,
        TaskState::InReview,
        TaskState::Working,
        TaskState::Queued,
        TaskState::Draft,
        TaskState::Dropped,
    ] {
        let n = c.get(s);
        if n > 0 {
            let word = match s {
                TaskState::Draft => "in draft".to_string(),
                other => other.label().to_lowercase(),
            };
            parts.push(format!("{n} {word}"));
        }
    }
    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(status: &str) -> TaskFacts {
        TaskFacts {
            task_id: "T1".into(),
            title: "Limit login attempts".into(),
            status: status.into(),
            ..Default::default()
        }
    }

    fn run(id: &str, status: &str, at: &str) -> RunFacts {
        RunFacts {
            run_id: id.into(),
            status: status.into(),
            created_at: at.into(),
            updated_at: at.into(),
            cost_usd: 2.5,
            ..Default::default()
        }
    }

    fn pr(n: u64, outcome: &str) -> PrFacts {
        PrFacts {
            repo: "example-org/web".into(),
            number: n,
            url: format!("https://github.com/example-org/web/pull/{n}"),
            outcome: outcome.into(),
        }
    }

    #[test]
    fn filing_the_ticket_is_never_done() {
        // The row the worker writes once the ticket is filed, old and new.
        for status in ["filed", "done", "issue_created"] {
            let v = task_view(&task(status), &[], None);
            assert_eq!(v.state, TaskState::Queued, "{status}");
            assert!(v.ticket_filed);
            assert!(v.detail.contains("waiting for its run"), "{}", v.detail);
        }
    }

    #[test]
    fn the_state_follows_the_run_and_its_pull_request() {
        let t = task("filed");
        let mut r = run("R1", "running", "2026-11-01T10:00:00Z");
        r.current_pass = "implement".into();
        assert_eq!(task_view(&t, &[r.clone()], None).state, TaskState::Working);

        r.status = "completed".into();
        r.prs = vec![pr(12, "")];
        let v = task_view(&t, &[r.clone()], None);
        assert_eq!(v.state, TaskState::InReview);
        assert_eq!(v.prs[0].number, 12);

        r.ci_last = "failed".into();
        r.status = "failed".into();
        r.error = "lint fails".into();
        let v = task_view(&t, &[r.clone()], None);
        assert_eq!(v.state, TaskState::NeedsYou);
        assert_eq!(v.needs.as_ref().unwrap().kind, "checks");
        assert_eq!(v.prs[0].tone, "failing");

        r.status = "completed".into();
        r.ci_last = "passed".into();
        r.standing = Some(crate::merge_standing::decide(
            "example-org/web",
            &crate::merge_standing::Facts {
                auto_merge: true,
                require_human_approval: true,
                ..Default::default()
            },
        ));
        let v = task_view(&t, &[r.clone()], None);
        assert_eq!(v.state, TaskState::NeedsYou);
        assert_eq!(v.needs.as_ref().unwrap().ask, "Approve pull request #12");

        r.pr_outcome = "merged".into();
        r.prs = vec![pr(12, "merged")];
        let v = task_view(&t, &[r.clone()], None);
        assert_eq!(v.state, TaskState::Merged);
        assert_eq!(v.prs[0].tone, "merged");

        r.pr_outcome = "closed".into();
        r.prs = vec![pr(12, "closed")];
        r.standing = None;
        let v = task_view(&t, &[r], None);
        assert_eq!(v.state, TaskState::Failed);
        assert!(v.reason.contains("closed without merging"));
    }

    #[test]
    fn a_failure_keeps_its_reason_and_counts_every_attempt() {
        let t = task("filed");
        let mut a = run("R1", "retried", "2026-11-01T10:00:00Z");
        a.prs = vec![pr(1, "closed")];
        let mut b = run("R2", "failed", "2026-11-02T10:00:00Z");
        b.error = "Reached the maximum number of steps".into();
        let v = task_view(&t, &[b.clone(), a.clone()], None);
        assert_eq!(v.state, TaskState::Failed);
        assert_eq!(v.reason, "Reached the maximum number of steps");
        assert_eq!(v.attempts, 2);
        assert!((v.cost_usd - 5.0).abs() < 1e-9);
        assert_eq!(v.runs[0].run_id, "R1", "oldest first");
        let mut row = task("failed");
        row.failure_reason = "No Jira project is set for this task.".into();
        let v = task_view(&row, &[], None);
        assert_eq!(v.reason, "No Jira project is set for this task.");
    }

    #[test]
    fn a_dropped_or_draft_task_says_so() {
        assert_eq!(
            task_view(&task("rejected"), &[], None).state,
            TaskState::Dropped
        );
        assert_eq!(task_view(&task("draft"), &[], None).state, TaskState::Draft);
        let v = task_view(&task("waiting"), &[], Some("Add the table"));
        assert_eq!(v.state, TaskState::Queued);
        assert!(v.detail.contains("Add the table"));
    }

    #[test]
    fn one_approved_task_leaves_the_plan_open_with_the_rest_to_approve() {
        let a = task("filed");
        let mut b = task("draft");
        b.task_id = "T2".into();
        let mut c = task("draft");
        c.task_id = "T3".into();
        let va = task_view(&a, &[run("R1", "running", "2026-11-01T10:00:00Z")], None);
        let (vb, vc) = (task_view(&b, &[], None), task_view(&c, &[], None));
        let p = plan_view(&[(&a, &va), (&b, &vb), (&c, &vc)], "2026-11-01T11:00:00Z");
        assert_eq!(p.state, PlanState::Running);
        assert!(p
            .needs
            .iter()
            .any(|n| n.kind == "approve_rest" && n.ask.contains("2 tasks")));
        // Once the approved one merged, the drafts are what is left.
        let mut done = run("R1", "merged", "2026-11-01T10:00:00Z");
        done.pr_outcome = "merged".into();
        let va = task_view(&a, &[done], None);
        let p = plan_view(&[(&a, &va), (&b, &vb), (&c, &vc)], "2026-11-01T11:00:00Z");
        assert_eq!(p.state, PlanState::NeedsYou);
        assert_ne!(p.state, PlanState::Done);
    }

    #[test]
    fn done_means_every_task_merged_or_dropped() {
        let a = task("filed");
        let mut b = task("rejected");
        b.task_id = "T2".into();
        let mut m = run("R1", "completed", "2026-11-01T10:00:00Z");
        m.prs = vec![pr(4, "")];
        let open = task_view(&a, &[m.clone()], None);
        let dropped = task_view(&b, &[], None);
        let p = plan_view(&[(&a, &open), (&b, &dropped)], "2026-11-01T11:00:00Z");
        assert_eq!(p.state, PlanState::Running);
        m.prs = vec![pr(4, "merged")];
        let merged = task_view(&a, &[m], None);
        let p = plan_view(&[(&a, &merged), (&b, &dropped)], "2026-11-01T11:00:00Z");
        assert_eq!(p.state, PlanState::Done);
        assert_eq!(p.summary, "1 of 2 merged, 1 dropped");
        let c = Counts {
            draft: 3,
            dropped: 1,
            ..Default::default()
        };
        assert_eq!(summary(&c), "3 tasks to review, 1 dropped");
    }

    #[test]
    fn open_work_with_no_activity_is_stalled() {
        let a = task("filed");
        let mut r = run("R1", "completed", "2026-11-01T10:00:00Z");
        r.prs = vec![pr(4, "")];
        let v = task_view(&a, &[r], None);
        let p = plan_view(&[(&a, &v)], "2026-11-09T10:00:00Z");
        assert_eq!(p.state, PlanState::Stalled);
        let p = plan_view(&[(&a, &v)], "2026-11-02T10:00:00Z");
        assert_eq!(p.state, PlanState::Running);
    }

    #[test]
    fn drift_marks_changes_after_approval_and_work_outside_scope() {
        let mut t = task("filed");
        t.created_at = "2026-11-02T00:00:00Z".into();
        t.added_by_kind = "person".into();
        t.edited_at = "2026-11-03T00:00:00Z".into();
        t.edited_by_kind = "person".into();
        t.description = "Change `apps/web/src/login/limit.ts` only.".into();
        let mut r = run("R1", "completed", "2026-11-04T00:00:00Z");
        r.prs = vec![pr(9, "")];
        r.files = vec![
            "apps/web/src/login/limit.ts".into(),
            "apps/web/src/login/limit.test.ts".into(),
            "apps/web/src/billing/charge.ts".into(),
        ];
        r.scope_moved_in = vec![("the audit log".into(), "criterion 2 asks for it".into())];
        let v = task_view(&t, &[r.clone()], None);
        let d = task_drift(&t, &v, &[r], "2026-11-01T00:00:00Z");
        let kinds: Vec<&str> = d.iter().map(|d| d.kind.as_str()).collect();
        assert_eq!(kinds, ["added", "changed", "scope", "scope"]);
        assert!(d[2].detail.contains("apps/web/src/billing/charge.ts"));
        assert!(!d[2].detail.contains("limit.test.ts"));
        // Before the plan ran, nothing is drift.
        assert!(task_drift(&t, &v, &[], "").is_empty());
        // No paths named: no written scope to leave.
        assert!(outside_scope("Make login safer", &["a/b.rs".into()]).is_empty());
    }

    #[test]
    fn criteria_come_with_their_evidence() {
        use crate::trace::{Proof, Row};
        let mut t = task("filed");
        t.acceptance_criteria =
            "- The 11th attempt in 15 minutes gets a 429\n- The page shows when to retry".into();
        let tr = RunTrace {
            run_id: "R1".into(),
            trace: Trace {
                head: "abc".into(),
                rows: vec![Row {
                    criterion: "The 11th attempt in 15 minutes gets a 429".into(),
                    source: "ticket".into(),
                    proof: Proof {
                        state: ProofState::Proven,
                        by: "tests with and without the change".into(),
                        detail: String::new(),
                    },
                    ..Default::default()
                }],
            },
        };
        let c = criteria(&[], &[(&t, Some(&tr))]);
        assert_eq!(c.len(), 2);
        assert!(c[0].met && c[0].evidence[0].state == "proven");
        assert!(!c[1].met && c[1].evidence.is_empty());
        // A plan's own list: tasks name what they cover.
        t.covers = vec!["C2".into()];
        let c = criteria(
            &[
                "Logins are limited".into(),
                "People see when to retry".into(),
            ],
            &[(&t, None)],
        );
        assert!(c[0].tasks.is_empty());
        assert_eq!(c[1].tasks, ["T1"]);
    }

    #[test]
    fn the_chip_colour_follows_checks_and_review() {
        let mut r = run("R1", "completed", "t");
        r.prs = vec![pr(3, "")];
        r.ci_last = "passed".into();
        assert_eq!(chip(&r, &r.prs[0].clone()).tone, "ready");
        r.review = "changes_requested".into();
        assert_eq!(chip(&r, &r.prs[0].clone()).tone, "failing");
        r.review.clear();
        r.status = "awaiting_ci".into();
        r.ci_last.clear();
        assert_eq!(chip(&r, &r.prs[0].clone()).tone, "waiting");
    }
}
