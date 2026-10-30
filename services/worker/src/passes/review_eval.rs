//! Reviewer behavior evals: real review histories replayed as regression
//! cases (anonymized fixtures in `review_cases/`, see its README).
//!
//! Deterministic layer (every PR, no model): each case drives the real
//! decision code (`decide_verdict`, the enforcement rules, the one verdict
//! function in `review_verdict`, the failure/requeue policy, the job dedup
//! keys, the stuck-review sweep, the reply-after-review rule) with the model
//! outputs recorded in the case, and checks the end state and the
//! invariants:
//!   (a) no formal verdict is ever posted for a review that didn't finish;
//!   (b) no reply that concludes "nothing blocks" is posted while
//!       CoderHelm's own request for changes stands;
//!   (c) every head ends with a formal outcome, and a redelivered job never
//!       posts twice.
//!
//! Merge-gate steps (approvals, dismissals, CI results, a person's draft)
//! drive the real gate decision (`await_merge::decide_gate`), the re-arm
//! rules the gateway applies (`common::merge_gate::wakes_on_*`), the approval
//! reading (`review_actions::approval_state`) and the draft guard
//! (`human_converted_to_draft`), and check one more invariant:
//!   (d) the gate never merges a draft, a PR whose CoderHelm approval was
//!       dismissed or never given, or one whose CI is running or failing.
//!
//! Live layer (`cargo test -p worker review_eval -- --ignored`, nightly): the
//! model reads each recorded comment and reply, and its classification must
//! land in the case's outcome class; a case's `live_review` diff is reviewed
//! with the reviewer's own prompt, tools (answered from the case's files) and
//! critic, and its findings must meet the case's constraints.

use super::await_merge::{classify_check_runs, decide_gate, Action, GateView};
use super::review_actions::approval_state;
use super::review_agent::{self, Finding, ReviewOutput, VerdictChange};
use super::review_pr::{
    failure_plan, reply_after_formal, retry_message, FailurePlan, FormalResult,
};
use super::review_sweep::{stuck, FailureNote, ReviewRecord};
use super::review_verdict::{decide, Context, Decision, Input, StandingReview};
use crate::clients::github::human_converted_to_draft;
use crate::models::ReviewMessage;
use common::merge_gate;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    self_authored: bool,
    #[serde(default)]
    require_tests: bool,
    /// The repo's auto-merge policy; absent = auto-merge off (no gate).
    #[serde(default)]
    auto_merge: Option<AutoMerge>,
    steps: Vec<Step>,
    expect: Expect,
    /// A diff the live layer reviews with the real reviewer prompt and critic.
    #[serde(default)]
    live_review: Option<LiveReview>,
}

/// A small anonymized PR the live layer reviews end to end: the reviewer's
/// own system prompt, its repository tools answered from `files`, the critic
/// pass and the enforcement rules, then `expect` on what is left.
#[derive(Deserialize)]
pub(super) struct LiveReview {
    pub(super) title: String,
    #[serde(default)]
    pub(super) body: String,
    /// The diff as the reviewer sees it (`format_diff`'s shape).
    pub(super) diff: String,
    /// The files at the PR head the tools can read, by path.
    #[serde(default)]
    pub(super) files: std::collections::BTreeMap<String, String>,
    pub(super) expect: LiveExpect,
    /// The same PR re-reviewed after a person replied under an earlier
    /// CoderHelm comment.
    #[serde(default)]
    pub(super) after_reply: Option<AfterReply>,
}

#[derive(Deserialize)]
pub(super) struct AfterReply {
    /// The earlier CoderHelm comment, as posted.
    thread: PriorThread,
    /// What the person wrote under it.
    author: String,
    reply: String,
    expect: LiveExpect,
}

#[derive(Deserialize)]
struct PriorThread {
    path: String,
    line: u64,
    body: String,
}

/// What must hold for the findings left after the critic and enforcement.
#[derive(Deserialize, Default)]
pub(super) struct LiveExpect {
    /// Nothing holds the merge (the verdict isn't REQUEST_CHANGES).
    #[serde(default)]
    no_blocking: bool,
    /// No finding of these categories survives.
    #[serde(default)]
    no_category: Vec<String>,
    /// A finding of this category survives and holds the merge.
    #[serde(default)]
    require_category: Option<String>,
    /// Every earlier thread is reported settled (fixed, answered, accepted).
    #[serde(default)]
    prior_settled: bool,
}

#[derive(Deserialize, Clone)]
struct AutoMerge {
    #[serde(default = "yes")]
    require_human: bool,
}

fn yes() -> bool {
    true
}

#[derive(Deserialize)]
struct CiStep {
    name: String,
    status: String,
    #[serde(default)]
    conclusion: String,
    /// The check suite finished with this run (GitHub sends `check_suite
    /// completed`).
    #[serde(default)]
    suite_completed: bool,
}

#[derive(Deserialize)]
struct GateIs {
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    merged: Option<bool>,
    #[serde(default)]
    draft: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Step {
    Push(String),
    Review(ReviewStep),
    ReviewFails {
        head: String,
        times: u32,
    },
    LegacyErrorReview {
        head: String,
        error: String,
    },
    Comment(CommentStep),
    Sweep {
        minutes_later: i64,
        review: Option<ReviewStep>,
    },
    /// A person approves the PR (GitHub `pull_request_review` approved).
    Approve {
        by: String,
    },
    /// A person requests changes.
    RequestChanges {
        by: String,
    },
    /// A review is dismissed (`who` = `coderhelm` or a person's login).
    Dismiss {
        who: String,
    },
    /// A check run on the head changes.
    Ci(CiStep),
    /// A person converts the PR back to draft.
    ConvertToDraft {
        by: String,
    },
    /// CI passed on CoderHelm's branch: the pipeline tries to mark the PR
    /// ready (the MarkReady path).
    CiPassedMarkReady {},
    /// Assert the gate's state mid-case.
    GateIs(GateIs),
}

#[derive(Deserialize, Clone)]
struct ReviewStep {
    head: String,
    model_output: Value,
    #[serde(default = "one")]
    deliveries: u32,
}

fn one() -> u32 {
    1
}

#[derive(Deserialize)]
struct CommentStep {
    id: u64,
    text: String,
    ask_kind: String,
    #[serde(default)]
    reply: Option<String>,
    #[serde(default)]
    conclusion: Option<Value>,
    #[serde(default)]
    review: Option<ReviewStep>,
}

#[derive(Deserialize)]
struct Expect {
    #[serde(default)]
    final_formal: Option<FinalFormal>,
    #[serde(default)]
    formal_events: Option<Vec<(String, String)>>,
    #[serde(default)]
    first_approval_has_notes: bool,
    /// The merge gate's final state, whether it merged, whether the PR is a draft.
    #[serde(default)]
    gate: Option<GateIs>,
}

#[derive(Deserialize)]
struct FinalFormal {
    head: String,
    event: String,
}

/// An open CoderHelm thread on the PR.
struct Thread {
    holds: bool,
}

/// A formal review CoderHelm posted.
#[derive(Debug, Clone)]
struct Formal {
    head: String,
    event: &'static str,
    notes: usize,
    at: i64,
}

#[derive(Default)]
struct Sim {
    head: String,
    self_authored: bool,
    require_tests: bool,
    /// Minutes since the case started.
    now: i64,
    formal: Vec<Formal>,
    /// The old error review, standing until a formal review replaces it.
    legacy_error: Option<(String, i64, String)>,
    threads: Vec<Thread>,
    claims: HashSet<String>,
    failure: Option<FailureNote>,
    replies: Vec<String>,
    /// Asks handed to the fix loop (CoderHelm's own PRs).
    fix_loop: Vec<String>,
    violations: Vec<String>,
    // ── merge gate ──
    auto_merge: Option<AutoMerge>,
    /// CoderHelm's recorded verdict per head (stored even when not posted).
    bot_verdicts: std::collections::HashMap<String, String>,
    /// GitHub's review list, oldest first.
    gh_reviews: Vec<Value>,
    checks: Vec<Value>,
    timeline: Vec<Value>,
    draft: bool,
    /// The gate record: (head, state). None = never armed.
    gate: Option<(String, String)>,
    merged: bool,
}

fn ts(minutes: i64) -> String {
    (chrono::DateTime::<chrono::Utc>::from_timestamp(1_800_000_000, 0).unwrap()
        + chrono::Duration::minutes(minutes))
    .to_rfc3339()
}

impl Sim {
    fn standing(&self) -> Option<StandingReview> {
        let last_formal = self
            .formal
            .iter()
            .rev()
            .find(|f| f.event != "COMMENT")
            .cloned();
        match (&self.legacy_error, last_formal) {
            (Some((head, at, _)), Some(f)) if *at > f.at => Some(StandingReview {
                verdict: "REQUEST_CHANGES".into(),
                failed: true,
                head_sha: head.clone(),
            }),
            (Some((head, _, _)), None) => Some(StandingReview {
                verdict: "REQUEST_CHANGES".into(),
                failed: true,
                head_sha: head.clone(),
            }),
            (_, Some(f)) => Some(StandingReview {
                verdict: f.event.to_string(),
                failed: false,
                head_sha: f.head.clone(),
            }),
            (None, None) => None,
        }
    }

    fn records(&self) -> Vec<ReviewRecord> {
        let mut r: Vec<(i64, ReviewRecord)> = self
            .formal
            .iter()
            .map(|f| {
                (
                    f.at,
                    ReviewRecord {
                        verdict: f.event.into(),
                        posted_as: f.event.into(),
                        head_sha: f.head.clone(),
                        created_at: ts(f.at),
                        body: "review".into(),
                    },
                )
            })
            .collect();
        if let Some((head, at, error)) = &self.legacy_error {
            r.push((
                *at,
                ReviewRecord {
                    verdict: "REQUEST_CHANGES".into(),
                    posted_as: "REQUEST_CHANGES".into(),
                    head_sha: head.clone(),
                    created_at: ts(*at),
                    body: format!(
                        "{} ({error}). Requesting a human look.",
                        super::review_sweep::LEGACY_FAILED_PREFIX
                    ),
                },
            ));
        }
        r.sort_by_key(|(at, _)| std::cmp::Reverse(*at));
        r.into_iter().map(|(_, r)| r).collect()
    }

    fn ctx(&self) -> Context {
        Context {
            standing: self.standing(),
            self_authored: self.self_authored,
        }
    }

    fn job(&self, head: &str, dedup_key: &str) -> ReviewMessage {
        ReviewMessage {
            team_id: "T".into(),
            installation_id: 1,
            repo_owner: "acme".into(),
            repo_name: "app".into(),
            pr_number: 7,
            head_sha: head.into(),
            label: "ch-review".into(),
            question: None,
            trigger: "synchronize".into(),
            dedup_key: dedup_key.into(),
            reply_to_comment_id: None,
            thread_root_id: None,
            mentioned: false,
            requested_by: String::new(),
            attempt: 0,
            wait_secs: 0,
            reconcile_note: None,
            outstanding_work: None,
        }
    }

    /// Takes a job's dedup claim: a redelivered copy gets false.
    fn claim(&mut self, key: &str) -> bool {
        self.claims.insert(key.to_string())
    }

    /// A review that finished: the real rules from findings to verdict to
    /// what is posted. Returns the event posted, if any.
    fn finished_review(&mut self, step: &ReviewStep, explicit: bool) -> Option<&'static str> {
        assert_eq!(
            step.head, self.head,
            "a review step must be of the current head"
        );
        let out: ReviewOutput =
            serde_json::from_value(step.model_output.clone()).expect("model_output is a review");
        let mut findings: Vec<Finding> = out.findings.clone();
        if self.require_tests {
            review_agent::enforce_required_tests(&mut findings);
        }
        super::review_severity::ground(&mut findings, "");
        review_agent::enforce_source_data_override(&mut findings);
        review_agent::enforce_manual_checks_are_notes(&mut findings);
        // Earlier threads the review reports settled close.
        let settled: HashSet<usize> = out
            .prior
            .iter()
            .filter(|p| p.is_settled())
            .map(|p| p.index)
            .collect();
        let mut i = 0;
        self.threads.retain(|_| {
            let keep = !settled.contains(&i);
            i += 1;
            keep
        });
        let holding = self.threads.iter().filter(|t| t.holds).count();
        let unconfirmed = review_agent::unconfirmed_external_facts("", &out.unconfirmed_facts);
        let verdict = review_agent::decide_verdict(&findings, holding, &unconfirmed, false);
        let prev = self.standing();
        let has_news = prev.as_ref().map(|p| p.verdict.as_str()) != Some(verdict)
            || prev.as_ref().is_some_and(|p| p.head_sha != step.head)
            || !findings.is_empty()
            || !settled.is_empty();
        let decision = decide(
            Input::Review {
                verdict,
                has_news,
                explicit,
                already_answered: false,
            },
            &self.ctx(),
        );
        self.bot_verdicts
            .insert(step.head.clone(), verdict.to_string());
        // New findings become open threads.
        for f in &findings {
            self.threads.push(Thread {
                holds: f.holds_merge(),
            });
        }
        match decision {
            Decision::Post { event, .. } => {
                self.now += 1;
                self.formal.push(Formal {
                    head: step.head.clone(),
                    event,
                    notes: findings.iter().filter(|f| !f.holds_merge()).count(),
                    at: self.now,
                });
                let gh_state = match event {
                    "APPROVE" => "APPROVED",
                    "REQUEST_CHANGES" => "CHANGES_REQUESTED",
                    _ => "COMMENTED",
                };
                self.gh_reviews.push(serde_json::json!({
                    "user": {"login": "coderhelm[bot]"},
                    "state": gh_state,
                    "commit_id": step.head,
                }));
                // run_on_approve arms the gate on every APPROVE verdict.
                if verdict == "APPROVE" {
                    self.arm_gate();
                }
                Some(event)
            }
            Decision::Nothing => {
                if verdict == "APPROVE" {
                    self.arm_gate();
                }
                None
            }
            Decision::Review => panic!("a finished review never asks for another"),
        }
    }

    /// The gateway re-arms an existing gate on this head when `wake` accepts
    /// its state.
    fn rearm_gate(&mut self, wake: fn(&str) -> bool) {
        if let Some((head, state)) = &self.gate {
            if *head == self.head && wake(state) {
                self.arm_gate();
            }
        }
    }

    /// One evaluation of the real gate decision.
    fn arm_gate(&mut self) {
        let Some(cfg) = self.auto_merge.clone() else {
            return;
        };
        if self.merged {
            return;
        }
        let mut view = GateView {
            draft: self.draft,
            require_human: cfg.require_human || self.self_authored,
            bot_verdict: self.bot_verdicts.get(&self.head).cloned(),
            approvals: approval_state(&Value::Array(self.gh_reviews.clone()), &self.head),
            want_labels: vec![],
            present_labels: vec![],
            labels_added_at: None,
            ci: None,
            now: 1_800_000_000 + self.now * 60,
        };
        let mut d = decide_gate(&view);
        if d.action == Action::ReadCi {
            view.ci = Some(classify_check_runs(&self.checks));
            d = decide_gate(&view);
        }
        let state = if d.action == Action::Merge {
            // Invariant (d), checked against the raw inputs, not the gate.
            let ci = classify_check_runs(&self.checks);
            let approvals = approval_state(&Value::Array(self.gh_reviews.clone()), &self.head);
            if self.draft
                || approvals.bot_dismissed_at_head
                || self.bot_verdicts.get(&self.head).map(String::as_str) != Some("APPROVE")
                || !ci.pending.is_empty()
                || !ci.failing.is_empty()
            {
                self.violations.push(format!(
                    "the gate merged {} while it was a draft, unapproved by CoderHelm, or CI was not green",
                    self.head
                ));
            }
            self.merged = true;
            merge_gate::STATE_MERGED.to_string()
        } else {
            d.state.to_string()
        };
        self.gate = Some((self.head.clone(), state));
    }

    fn gate_check(&mut self, case: &str, want: &GateIs) {
        let state = self.gate.as_ref().map(|(_, s)| s.as_str()).unwrap_or("");
        if let Some(w) = &want.state {
            if w != state {
                self.violations
                    .push(format!("{case}: the gate is {state:?}, want {w:?}"));
            }
        }
        if let Some(w) = want.merged {
            if w != self.merged {
                self.violations
                    .push(format!("{case}: merged is {}, want {w}", self.merged));
            }
        }
        if let Some(w) = want.draft {
            if w != self.draft {
                self.violations
                    .push(format!("{case}: draft is {}, want {w}", self.draft));
            }
        }
    }

    /// Delivers a review job `deliveries` times; only the first runs.
    fn deliver_review(
        &mut self,
        key: &str,
        step: &ReviewStep,
        explicit: bool,
    ) -> Option<&'static str> {
        let mut posted = None;
        for _ in 0..step.deliveries.max(1) {
            if self.claim(key) {
                posted = self.finished_review(step, explicit);
            }
        }
        posted
    }

    /// Runs the case and returns the violations found.
    fn run(case: &Case) -> Sim {
        let mut sim = Sim {
            self_authored: case.self_authored,
            require_tests: case.require_tests,
            auto_merge: case.auto_merge.clone(),
            ..Default::default()
        };
        // The dedup key of the job that reviews the current head next.
        let mut next_key = String::new();
        for step in &case.steps {
            sim.now += 5;
            match step {
                Step::Push(h) => {
                    sim.head = h.clone();
                    next_key = format!("head#{h}");
                }
                Step::Review(r) => {
                    let key = next_key.clone();
                    sim.deliver_review(&key, r, false);
                }
                Step::ReviewFails { head, times } => {
                    let mut job = sim.job(head, &next_key);
                    for _ in 0..*times {
                        assert!(sim.claim(&job.dedup_key), "a retry always has a fresh key");
                        let before = sim.formal.len();
                        if decide(Input::Failed, &sim.ctx()) != Decision::Nothing {
                            sim.violations
                                .push(format!("{}: a failed review posted a verdict", case.name));
                        }
                        assert_eq!(sim.formal.len(), before);
                        match failure_plan(job.attempt) {
                            FailurePlan::Retry { delay_secs } => {
                                let (next, _) = retry_message(&job, head, delay_secs);
                                sim.now += (delay_secs / 60) as i64;
                                job = next;
                                sim.failure = Some(FailureNote {
                                    head_sha: head.clone(),
                                    gave_up: false,
                                    updated_at: ts(sim.now),
                                });
                            }
                            FailurePlan::GiveUp => {
                                sim.failure = Some(FailureNote {
                                    head_sha: head.clone(),
                                    gave_up: true,
                                    updated_at: ts(sim.now),
                                });
                                break;
                            }
                        }
                    }
                    next_key = job.dedup_key.clone();
                }
                Step::LegacyErrorReview { head, error } => {
                    sim.legacy_error = Some((head.clone(), sim.now, error.clone()));
                }
                Step::Comment(c) => sim.comment(c),
                Step::Sweep {
                    minutes_later,
                    review,
                } => {
                    sim.now += minutes_later;
                    let now = chrono::DateTime::parse_from_rfc3339(&ts(sim.now))
                        .unwrap()
                        .with_timezone(&chrono::Utc);
                    let picked = stuck(&sim.records(), sim.failure.as_ref(), &sim.head, now);
                    match (picked, review) {
                        (Some(_), Some(r)) => {
                            let key = format!("sweep#{}", sim.head);
                            sim.deliver_review(&key, r, true);
                        }
                        (Some(_), None) => sim.violations.push(format!(
                            "{}: the sweep picked the PR but the case has no review for it",
                            case.name
                        )),
                        (None, Some(_)) => sim
                            .violations
                            .push(format!("{}: the sweep didn't pick a stuck PR", case.name)),
                        (None, None) => {}
                    }
                }
                Step::Approve { by } => {
                    sim.gh_reviews.push(serde_json::json!({
                        "user": {"login": by}, "state": "APPROVED", "commit_id": sim.head,
                    }));
                    // A person's approval always arms the gate.
                    sim.arm_gate();
                }
                Step::RequestChanges { by } => {
                    sim.gh_reviews.push(serde_json::json!({
                        "user": {"login": by}, "state": "CHANGES_REQUESTED", "commit_id": sim.head,
                    }));
                    sim.rearm_gate(merge_gate::wakes_on_review);
                }
                Step::Dismiss { who } => {
                    let login = if who == "coderhelm" {
                        "coderhelm[bot]".to_string()
                    } else {
                        who.clone()
                    };
                    sim.gh_reviews.push(serde_json::json!({
                        "user": {"login": login}, "state": "DISMISSED", "commit_id": sim.head,
                    }));
                    sim.rearm_gate(merge_gate::wakes_on_review);
                }
                Step::Ci(c) => {
                    sim.checks.push(serde_json::json!({
                        "name": c.name,
                        "status": c.status,
                        "conclusion": c.conclusion,
                        "started_at": ts(sim.now),
                    }));
                    if c.suite_completed {
                        sim.rearm_gate(merge_gate::wakes_on_ci);
                    }
                }
                Step::ConvertToDraft { by } => {
                    sim.draft = true;
                    sim.timeline.push(serde_json::json!({
                        "event": "convert_to_draft", "actor": {"login": by, "type": "User"},
                    }));
                }
                Step::CiPassedMarkReady {} => {
                    let timeline = Value::Array(sim.timeline.clone());
                    if sim.draft && human_converted_to_draft(&timeline).is_none() {
                        sim.draft = false;
                        sim.timeline.push(serde_json::json!({
                            "event": "ready_for_review",
                            "actor": {"login": "coderhelm[bot]", "type": "Bot"},
                        }));
                        sim.rearm_gate(merge_gate::wakes_on_review);
                    }
                }
                Step::GateIs(want) => sim.gate_check(&case.name, want),
            }
        }
        if let Some(want) = &case.expect.gate {
            sim.gate_check(&case.name, want);
        }
        sim
    }

    fn comment(&mut self, c: &CommentStep) {
        let key = format!("reply#{}", c.id);
        if !self.claim(&key) {
            return;
        }
        let kind = review_agent::parse_ask(&serde_json::json!({ "kind": c.ask_kind }));
        match review_agent::route_ask(kind, self.self_authored) {
            review_agent::AskRoute::FormalReview => {
                if let Some(r) = &c.review {
                    self.finished_review(r, true);
                }
                return;
            }
            review_agent::AskRoute::FixLoop => {
                self.fix_loop.push(c.text.clone());
                return;
            }
            review_agent::AskRoute::Reply => {}
        }
        let answer = c.reply.clone().unwrap_or_default();
        let conclusion =
            review_agent::parse_reply_conclusion(c.conclusion.as_ref().unwrap_or(&Value::Null));
        let decision = decide(
            Input::Reply {
                change: conclusion.verdict_change,
            },
            &self.ctx(),
        );
        let posted = if decision == Decision::Review {
            let r = c
                .review
                .as_ref()
                .expect("the case records the review this reply runs");
            let key = format!("reconcile#{}#{key}", r.head);
            let ev = self.deliver_review(&key, r, true);
            reply_after_formal(&answer, &self.head, FormalResult::Posted(ev))
        } else {
            answer.clone()
        };
        // Invariant (b): a reply that clears the PR never stands next to
        // CoderHelm's own request for changes.
        let blocks = self.standing().is_some_and(|s| s.blocks());
        if conclusion.verdict_change == VerdictChange::Clear && blocks && posted.contains(&answer) {
            self.violations.push(
                "a 'nothing blocks' reply was posted while CoderHelm's request for changes stands"
                    .into(),
            );
        }
        self.replies.push(posted);
    }
}

fn cases() -> Vec<(String, Case)> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/passes/review_cases");
    let mut out = vec![];
    for entry in std::fs::read_dir(&dir).expect("review_cases exists") {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let case: Case =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        out.push((
            path.file_name().unwrap().to_string_lossy().into_owned(),
            case,
        ));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    assert!(out.len() >= 5, "the cases are found");
    out
}

#[test]
fn review_eval_replays_every_case() {
    let mut failures = vec![];
    for (file, case) in cases() {
        let sim = Sim::run(&case);
        let mut fail = |m: String| failures.push(format!("{file}: {m}"));
        for v in &sim.violations {
            fail(v.clone());
        }
        let last = sim
            .formal
            .iter()
            .rev()
            .find(|f| f.event != "COMMENT")
            .or(sim.formal.last());
        match (&case.expect.final_formal, last) {
            (Some(want), Some(got)) => {
                if want.head != got.head || want.event != got.event {
                    fail(format!(
                        "ends with {} on {}, want {} on {}",
                        got.event, got.head, want.event, want.head
                    ));
                }
                // The head it ends on is the current head (c).
                if got.head != sim.head {
                    fail(format!(
                        "the current head {} has no formal outcome",
                        sim.head
                    ));
                }
            }
            (Some(_), None) => fail("no formal review was posted".into()),
            (None, Some(got)) => fail(format!("unexpected formal review {got:?}")),
            (None, None) => {}
        }
        if let Some(want) = &case.expect.formal_events {
            let got: Vec<(String, String)> = sim
                .formal
                .iter()
                .map(|f| (f.head.clone(), f.event.to_string()))
                .collect();
            if &got != want {
                fail(format!("formal reviews {got:?}, want {want:?}"));
            }
        }
        if case.expect.first_approval_has_notes {
            match sim.formal.iter().find(|f| f.event == "APPROVE") {
                Some(f) if f.notes > 0 => {}
                other => fail(format!("the approval should carry its notes: {other:?}")),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "reviewer eval failures:\n{}",
        failures.join("\n")
    );
}

/// The model the live evals call, and its id. `EVAL_PROVIDER=bedrock` sends
/// the same requests through Amazon Bedrock with the ambient AWS credentials
/// (the nightly workflow's narrow OIDC role, no API key); otherwise
/// ANTHROPIC_API_KEY is used. EVAL_MODEL picks the model (a Bedrock model or
/// inference profile id on Bedrock).
pub(super) async fn eval_client() -> (crate::agent::anthropic::AnthropicClient, String) {
    let meter = common::usage::UsageMeter::memory("").0;
    let bedrock = std::env::var("EVAL_PROVIDER").is_ok_and(|p| p == "bedrock");
    let (client, default_model) = if bedrock {
        let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        (
            crate::agent::anthropic::AnthropicClient::bedrock(
                aws_sdk_bedrockruntime::Client::new(&cfg),
                meter,
            ),
            "us.anthropic.claude-haiku-4-5-20251001-v1:0",
        )
    } else {
        let Ok(key) = std::env::var("ANTHROPIC_API_KEY") else {
            panic!("set EVAL_PROVIDER=bedrock (AWS credentials) or ANTHROPIC_API_KEY for the live reviewer evals");
        };
        (
            crate::agent::anthropic::AnthropicClient::new(key, meter),
            "claude-haiku-4-5",
        )
    };
    let model = std::env::var("EVAL_MODEL").unwrap_or_else(|_| default_model.into());
    (client, model)
}

/// Live layer: the model reads each recorded comment and reply; run nightly.
/// `EVAL_PROVIDER=bedrock` sends the same requests through Amazon Bedrock
/// with the ambient AWS credentials (the nightly workflow's narrow OIDC
/// role, no API key); otherwise ANTHROPIC_API_KEY is used. EVAL_MODEL picks
/// the model (a Bedrock model or inference profile id on Bedrock).
#[tokio::test]
#[ignore]
async fn review_eval_live_classification() {
    let (client, model) = eval_client().await;
    let mut usage = crate::models::TokenUsage::default();
    let mut failures = vec![];
    for (file, case) in cases() {
        for step in &case.steps {
            let Step::Comment(c) = step else { continue };
            let want = review_agent::parse_ask(&serde_json::json!({ "kind": c.ask_kind }));
            let v = crate::agent::anthropic::converse_json(
                &client,
                &model,
                review_agent::ASK_SYSTEM,
                &review_agent::ask_prompt(&c.text, "Changes requested.", None, None),
                &review_agent::ask_schema(),
                true,
                "low",
                512,
                &mut usage,
            )
            .await;
            match v.map(|v| review_agent::parse_ask(&v)) {
                Ok(got) if got.wants_formal_review() == want.wants_formal_review() => {}
                Ok(got) => failures.push(format!(
                    "{file} comment {}: classified {got:?}, want the class of {want:?}",
                    c.id
                )),
                Err(e) => failures.push(format!("{file} comment {}: {e}", c.id)),
            }
            if let (Some(reply), Some(concl)) = (&c.reply, &c.conclusion) {
                let want = review_agent::parse_reply_conclusion(concl).verdict_change;
                let v = crate::agent::anthropic::converse_json(
                    &client,
                    &model,
                    review_agent::REPLY_CONCLUSION_SYSTEM,
                    &review_agent::reply_conclusion_prompt(&[], &c.text, reply),
                    &review_agent::reply_conclusion_schema(),
                    true,
                    "low",
                    2048,
                    &mut usage,
                )
                .await;
                match v.map(|v| review_agent::parse_reply_conclusion(&v).verdict_change) {
                    Ok(got) if got == want => {}
                    Ok(got) => failures.push(format!(
                        "{file} reply {}: concluded {got:?}, want {want:?}",
                        c.id
                    )),
                    Err(e) => failures.push(format!("{file} reply {}: {e}", c.id)),
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "live reviewer eval failures:\n{}",
        failures.join("\n")
    );
}

// ── Live diff reviews ────────────────────────────────────────────────────
//
// A case's `live_review` is a small PR the model reviews the way production
// does: `review_agent::review_system_prompt` (every rule the reviewer runs
// with), the reviewer's tool list answered from the case's files instead of
// GitHub, the critic (`critic_system_prompt` / `parse_critic` /
// `apply_critic`), the enforcement rules and `decide_verdict`.

/// The reviewer's repository tools, answered from a case's files. Anything
/// outside them (org search, registries, the web) is not available here.
struct CaseFiles<'a>(&'a std::collections::BTreeMap<String, String>);

impl CaseFiles<'_> {
    fn answer(&self, name: &str, input: &Value) -> String {
        let path = input["path"]
            .as_str()
            .unwrap_or("")
            .trim_start_matches("./");
        match name {
            "read_file" => self
                .0
                .get(path)
                .cloned()
                .unwrap_or_else(|| format!("{path}: no such file at the PR head")),
            "read_file_lines" => {
                let Some(text) = self.0.get(path) else {
                    return format!("{path}: no such file at the PR head");
                };
                let start = input["start_line"].as_u64().unwrap_or(1).max(1) as usize;
                let end = input["end_line"].as_u64().unwrap_or(u64::MAX) as usize;
                text.lines()
                    .enumerate()
                    .filter(|(i, _)| *i + 1 >= start && *i < end)
                    .map(|(i, l)| format!("{}: {l}", i + 1))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
            "list_directory" => {
                let dir = path.trim_end_matches('/');
                let entries: Vec<&str> = self
                    .0
                    .keys()
                    .filter(|p| dir.is_empty() || p.starts_with(&format!("{dir}/")))
                    .map(String::as_str)
                    .collect();
                if entries.is_empty() {
                    format!("{path}: no such directory")
                } else {
                    entries.join("\n")
                }
            }
            "search_code" => {
                let q = input["query"].as_str().unwrap_or("");
                let hits: Vec<String> = self
                    .0
                    .iter()
                    .flat_map(|(p, text)| {
                        text.lines()
                            .enumerate()
                            .filter(|(_, l)| !q.is_empty() && l.contains(q))
                            .map(move |(i, l)| format!("{p}:{}: {}", i + 1, l.trim()))
                    })
                    .collect();
                if hits.is_empty() {
                    format!("no matches for {q:?}")
                } else {
                    hits.join("\n")
                }
            }
            _ => format!("{name} is not available in this review"),
        }
    }
}

#[async_trait::async_trait]
impl crate::agent::llm::ToolExecutor for CaseFiles<'_> {
    async fn execute(
        &self,
        name: &str,
        input: &Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Value::String(self.answer(name, input)))
    }
}

/// What a live review left: the findings after the critic and enforcement,
/// its verdict, and its report on earlier threads.
pub(super) struct LiveOutcome {
    pub(super) findings: Vec<Finding>,
    pub(super) verdict: &'static str,
    prior: Vec<review_agent::PriorStatus>,
    unconfirmed: Vec<review_agent::UnconfirmedFact>,
}

/// Reviews `lr` with the real prompts; `prior` is the earlier-threads section.
/// `model` reads the PR and writes the findings; `critic_model` runs the
/// critic, which decides which findings are kept (and so what blocks).
pub(super) async fn live_review(
    client: &crate::agent::anthropic::AnthropicClient,
    model: &str,
    critic_model: &str,
    lr: &LiveReview,
    prior: &str,
    usage: &mut crate::models::TokenUsage,
) -> Result<LiveOutcome, String> {
    // Built as production builds it (`review_pr::run_once`); the cases carry
    // no repo or team instructions, so both blocks are empty.
    let team_instructions = "";
    let instructions_block = format!(
        "{}{}",
        super::format_instructions_block(""),
        review_agent::team_rules_block(team_instructions)
    );
    let system = review_agent::review_system_prompt("acme", "app", &instructions_block, false);
    let prompt = review_agent::review_user_prompt(&lr.title, &lr.body, &lr.diff, prior);
    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];
    let tools = review_agent::review_tools();
    // An offline evaluation of saved cases: no team, read-only case files,
    // gated like every loop (on an in-memory policy, so reads run).
    let gate = common::agent_policy::Gate::new(
        std::sync::Arc::new(common::agent_policy::store::MemStore::new()),
        common::agent_policy::GateCtx::new("review-eval", common::agent_policy::Agent::Reviewer),
    )
    .offering(tools.iter().map(|t| t.name.clone()));
    let text = crate::agent::anthropic::converse_tool_loop(
        client,
        model,
        &system,
        &mut messages,
        &tools,
        &CaseFiles(&lr.files),
        &gate,
        usage,
        16,
        8192,
        None,
        None,
        None,
        None,
    )
    .await
    .map_err(|e| format!("the review call failed: {e}"))?;
    let out = review_agent::parse_review(&text)?;
    let mut findings = if out.findings.is_empty() {
        out.findings
    } else {
        // As in production, the critic sees settled threads and the PR
        // conversation (none here), not the open threads the review just
        // reported on.
        let ctx = review_agent::CriticContext {
            require_tests: false,
            answered: "",
            live_evidence: "",
            team_instructions,
            lessons: "",
        };
        let critic = crate::agent::anthropic::converse_simple(
            client,
            critic_model,
            &review_agent::critic_system_prompt(&ctx),
            &review_agent::critic_user_prompt(&lr.diff, &out.findings, &ctx),
            usage,
        )
        .await
        .ok()
        .and_then(|t| review_agent::parse_critic(&t));
        match critic {
            Some(kept) => review_agent::apply_critic(out.findings, &kept),
            None => out.findings,
        }
    };
    super::review_severity::ground(&mut findings, "");
    review_agent::enforce_source_data_override(&mut findings);
    review_agent::enforce_manual_checks_are_notes(&mut findings);
    let unconfirmed = review_agent::unconfirmed_external_facts(&lr.body, &out.unconfirmed_facts);
    let verdict = review_agent::decide_verdict(&findings, 0, &unconfirmed, false);
    Ok(LiveOutcome {
        findings,
        verdict,
        prior: out.prior,
        unconfirmed,
    })
}

/// Where `got` breaks `want`, one line each.
fn live_violations(want: &LiveExpect, got: &LiveOutcome) -> Vec<String> {
    let mut out = vec![];
    let show = |f: &Finding| {
        format!(
            "[{} {} {}] {}:{} {}",
            f.category,
            f.severity,
            f.evidence().as_str(),
            f.file,
            f.line,
            f.title
        )
    };
    if want.no_blocking {
        for f in got.findings.iter().filter(|f| f.holds_merge()) {
            out.push(format!("holds the merge ({}): {}", got.verdict, show(f)));
        }
        for f in got.unconfirmed.iter().filter(|f| f.blocks) {
            out.push(format!(
                "unconfirmed fact holds the merge ({}): {} ({})",
                got.verdict, f.fact, f.why
            ));
        }
    }
    for cat in &want.no_category {
        for f in got
            .findings
            .iter()
            .filter(|f| f.category.eq_ignore_ascii_case(cat))
        {
            out.push(format!("raised {cat}: {}", show(f)));
        }
    }
    if let Some(cat) = &want.require_category {
        if !got
            .findings
            .iter()
            .any(|f| f.category.eq_ignore_ascii_case(cat) && f.holds_merge())
        {
            let all: Vec<String> = got.findings.iter().map(show).collect();
            out.push(format!(
                "no blocking {cat} finding ({}); findings: {all:?}",
                got.verdict
            ));
        }
    }
    if want.prior_settled && !got.prior.iter().any(|p| p.index == 0 && p.is_settled()) {
        let statuses: Vec<String> = got
            .prior
            .iter()
            .map(|p| format!("{}={}", p.index, p.status))
            .collect();
        out.push(format!("the earlier comment is not settled: {statuses:?}"));
    }
    out
}

/// The earlier-threads section for a re-review after `r`.
fn prior_section(r: &AfterReply) -> String {
    let thread = crate::clients::github::BotReviewThread {
        root_id: 0,
        id: "t1".into(),
        path: r.thread.path.clone(),
        line: r.thread.line,
        outdated: false,
        body: r.thread.body.clone(),
        replies: vec![crate::clients::github::ThreadReply {
            author: r.author.clone(),
            by_bot: false,
            body: r.reply.clone(),
        }],
        resolved: false,
        resolved_by_person: false,
    };
    review_agent::format_prior_threads(&[&thread])
}

/// Every case's `live_review` is well formed, so a broken case fails here and
/// not at night.
#[test]
fn review_eval_live_reviews_are_well_formed() {
    let mut found = 0;
    for (file, case) in cases() {
        let Some(lr) = &case.live_review else {
            continue;
        };
        found += 1;
        assert!(lr.diff.contains("\n@@"), "{file}: the diff has a hunk");
        for path in lr.files.keys() {
            assert!(
                lr.diff.contains(&format!("### {path} ")),
                "{file}: {path} is in the diff"
            );
        }
        let checks = |e: &LiveExpect| {
            e.no_blocking || !e.no_category.is_empty() || e.require_category.is_some()
        };
        assert!(checks(&lr.expect), "{file}: live_review checks something");
        assert!(
            !(lr.expect.no_blocking && lr.expect.require_category.is_some()),
            "{file}: a case can't require a block and forbid one"
        );
        if let Some(r) = &lr.after_reply {
            assert!(checks(&r.expect), "{file}: after_reply checks something");
            assert!(
                prior_section(r).contains(&r.reply),
                "{file}: the reply is shown"
            );
        }
    }
    assert!(found >= 2, "both directions have a live review case");
}

#[test]
fn case_files_answer_the_reviewer_s_tools() {
    let files: std::collections::BTreeMap<String, String> = [(
        "sync/a.py".to_string(),
        "X = {1, 2}\n\ndef f():\n    return X\n".to_string(),
    )]
    .into();
    let t = CaseFiles(&files);
    assert!(t
        .answer("read_file", &serde_json::json!({"path": "sync/a.py"}))
        .contains("X = {1, 2}"));
    assert_eq!(
        t.answer(
            "read_file_lines",
            &serde_json::json!({"path": "sync/a.py", "start_line": 3, "end_line": 4})
        ),
        "3: def f():\n4:     return X"
    );
    assert!(t
        .answer("search_code", &serde_json::json!({"query": "X"}))
        .contains("sync/a.py:4:"));
    assert_eq!(
        t.answer("list_directory", &serde_json::json!({"path": "sync"})),
        "sync/a.py"
    );
    assert!(t
        .answer("read_file", &serde_json::json!({"path": "nope"}))
        .contains("no such file"));
    assert!(t
        .answer("search_org", &serde_json::json!({"query": "X"}))
        .contains("not available"));
}

/// Live layer for diff cases: the model reviews each case's diff with the
/// reviewer's own prompt, tools and critic, and what is left must meet the
/// case's `expect`; then, with `after_reply`, re-reviews it with the person's
/// reply under the earlier comment. EVAL_REVIEW_MODEL picks the reviewing
/// model (default EVAL_MODEL): a small model can pass a case the production
/// reviewer model fails, so judge the rules with the model that runs them.
#[tokio::test]
#[ignore]
async fn review_eval_live_diff_reviews() {
    let (client, model) = eval_client().await;
    let model = std::env::var("EVAL_REVIEW_MODEL").unwrap_or(model);
    let critic_model = std::env::var("EVAL_CRITIC_MODEL").unwrap_or_else(|_| model.clone());
    let mut usage = crate::models::TokenUsage::default();
    let mut failures = vec![];
    for (file, case) in cases() {
        let Some(lr) = &case.live_review else {
            continue;
        };
        match live_review(&client, &model, &critic_model, lr, "", &mut usage).await {
            Ok(got) => {
                println!(
                    "{file}: {} {:?}",
                    got.verdict,
                    got.findings
                        .iter()
                        .map(|f| format!("{}/{}", f.category, f.evidence().as_str()))
                        .collect::<Vec<_>>()
                );
                for v in live_violations(&lr.expect, &got) {
                    failures.push(format!("{file}: {v}"));
                }
            }
            Err(e) => failures.push(format!("{file}: {e}")),
        }
        if let Some(r) = &lr.after_reply {
            match live_review(
                &client,
                &model,
                &critic_model,
                lr,
                &prior_section(r),
                &mut usage,
            )
            .await
            {
                Ok(got) => {
                    println!(
                        "{file} after the reply: {} {:?} prior {:?}",
                        got.verdict,
                        got.findings
                            .iter()
                            .map(|f| format!("{}/{}", f.category, f.evidence().as_str()))
                            .collect::<Vec<_>>(),
                        got.prior
                            .iter()
                            .map(|p| p.status.clone())
                            .collect::<Vec<_>>()
                    );
                    for v in live_violations(&r.expect, &got) {
                        failures.push(format!("{file} after the reply: {v}"));
                    }
                }
                Err(e) => failures.push(format!("{file} after the reply: {e}")),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "live diff eval failures:\n{}",
        failures.join("\n")
    );
}

// ── Run cases (`review_cases/runs/`) ─────────────────────────────────────
//
// What a ticket run does around its review: a re-run of a ticket with
// history, a person's comment on the ticket, and the PR reviewer's verdict on
// the run's own PR. Each case drives the real decision code
// (`ticket_history::TicketHistory::guard`, `fix_location::route`,
// `jira_comment::decide`, the reviewer's enforcement rules and
// `decide_verdict`) with recorded inputs, and checks the invariants:
//   (e) a ticket whose CoderHelm PR was closed unmerged with an outside-code
//       lesson never gets another PR while the ticket is unchanged;
//   (f) a plan with no decision on where the fix belongs never codes;
//   (g) a ticket comment that is informational or a status note never
//       starts a run;
//   (h) a run never completes while its PR reviewer's verdict blocks.

#[derive(Deserialize)]
struct RunCase {
    #[allow(dead_code)]
    name: String,
    kind: String,
    #[serde(default)]
    history: Option<RunCaseHistory>,
    #[serde(default)]
    current_hash: String,
    #[serde(default)]
    plan: Option<super::fix_location::FixDecision>,
    #[serde(default)]
    run_status: String,
    #[serde(default)]
    action: String,
    #[serde(default)]
    comment: String,
    #[serde(default)]
    intent: String,
    #[serde(default)]
    model_output: Option<Value>,
    expect: Value,
}

#[derive(Deserialize, Default)]
struct RunCaseHistory {
    #[serde(default)]
    runs: Vec<RunCaseRun>,
    #[serde(default)]
    notes: Vec<common::ticket_state::Note>,
}

#[derive(Deserialize, Default)]
struct RunCaseRun {
    run_id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    repo: String,
    #[serde(default)]
    pr_number: u64,
    #[serde(default)]
    pr_outcome: String,
    #[serde(default)]
    cancel_reason: String,
    #[serde(default)]
    lesson: String,
    #[serde(default)]
    lesson_where: String,
    #[serde(default)]
    fix_location: String,
    #[serde(default)]
    context_hash: String,
}

fn run_cases() -> Vec<(String, RunCase)> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/passes/review_cases/runs");
    let mut out = vec![];
    for entry in std::fs::read_dir(&dir).expect("review_cases/runs exists") {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let case: RunCase =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        out.push((
            path.file_name().unwrap().to_string_lossy().into_owned(),
            case,
        ));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    assert!(out.len() >= 3, "the run cases are found");
    out
}

fn route_name(r: super::fix_location::Route) -> &'static str {
    use super::fix_location::Route;
    match r {
        Route::Code => "code",
        Route::OutsideOnly => "outside_only",
        Route::CodeAndOutside => "code_and_outside",
        Route::Ask => "ask",
    }
}

#[test]
fn review_eval_replays_every_run_case() {
    use super::fix_location::{self, Route};
    use super::jira_comment::{self, Act, ActionKind};
    use super::ticket_history::{Guard, PriorRun, TicketHistory};
    let mut failures = vec![];
    for (file, case) in run_cases() {
        let mut fail = |m: String| failures.push(format!("{file}: {m}"));
        let want = |k: &str| case.expect.get(k).cloned().unwrap_or(Value::Null);
        match case.kind.as_str() {
            "rerun" => {
                let h = case.history.unwrap_or_default();
                let history = TicketHistory {
                    runs: h
                        .runs
                        .into_iter()
                        .map(|r| PriorRun {
                            run_id: r.run_id,
                            status: r.status,
                            repo: r.repo,
                            pr_number: r.pr_number,
                            pr_outcome: r.pr_outcome,
                            cancel_reason: r.cancel_reason,
                            lesson: r.lesson,
                            lesson_where: r.lesson_where,
                            fix_location: r.fix_location,
                            context_hash: r.context_hash,
                            ..Default::default()
                        })
                        .collect(),
                    notes: h.notes,
                    done_hash: String::new(),
                    prs: vec![],
                    decisions: vec![],
                    prior: Default::default(),
                };
                // What the run does: the planner's route, then the history.
                let route = match history.guard(case.plan.as_ref(), &case.current_hash) {
                    Guard::Stop { route, .. } => route,
                    Guard::Proceed => fix_location::route(case.plan.as_ref()),
                };
                let new_pr = matches!(route, Route::Code | Route::CodeAndOutside);
                if !history.closed_unmerged().is_empty()
                    && history.outside_conclusion().is_some()
                    && new_pr
                {
                    fail(
                        "invariant (e): a second PR after a closed PR with an outside lesson"
                            .into(),
                    );
                }
                if case.plan.is_none() && new_pr {
                    fail("invariant (f): a plan with no decision coded".into());
                }
                if want("new_pr").as_bool() != Some(new_pr) {
                    fail(format!("new_pr = {new_pr}, want {}", want("new_pr")));
                }
                if let Some(r) = want("route").as_str() {
                    if r != route_name(route) {
                        fail(format!("route {}, want {r}", route_name(route)));
                    }
                }
            }
            "jira_comment" => {
                let kind = match case.action.as_str() {
                    "feedback" => ActionKind::Feedback,
                    "run" => ActionKind::Run,
                    _ => ActionKind::None,
                };
                let intent = common::ticket_state::Intent::parse(&case.intent);
                let act = jira_comment::decide(Some(intent), &case.run_status, kind);
                let starts_run = matches!(act, Act::Dispatch) && kind == ActionKind::Run;
                let act_name = match act {
                    Act::Dispatch => "dispatch",
                    Act::Elsewhere { .. } => "elsewhere",
                    Act::Nothing => "nothing",
                };
                if matches!(
                    intent,
                    common::ticket_state::Intent::FixElsewhereInfo
                        | common::ticket_state::Intent::StatusOnly
                ) && starts_run
                {
                    fail("invariant (g): an informational comment started a run".into());
                }
                if want("act").as_str() != Some(act_name) {
                    fail(format!("act {act_name}, want {}", want("act")));
                }
                if want("starts_run").as_bool() != Some(starts_run) {
                    fail(format!(
                        "starts_run = {starts_run}, want {}",
                        want("starts_run")
                    ));
                }
                assert!(
                    !case.comment.is_empty(),
                    "{file}: a comment case needs its comment"
                );
            }
            "own_pr_review" => {
                let out: ReviewOutput =
                    serde_json::from_value(case.model_output.clone().expect("model_output"))
                        .expect("model_output is a review");
                let mut findings = out.findings.clone();
                super::review_severity::ground(&mut findings, "");
                review_agent::enforce_source_data_override(&mut findings);
                review_agent::enforce_manual_checks_are_notes(&mut findings);
                let unconfirmed =
                    review_agent::unconfirmed_external_facts("", &out.unconfirmed_facts);
                let verdict = review_agent::decide_verdict(&findings, 0, &unconfirmed, false);
                let override_found = findings.iter().any(|f| {
                    f.category
                        .eq_ignore_ascii_case(review_agent::SOURCE_DATA_OVERRIDE)
                        && f.severity == "blocking"
                });
                // The run completes only when the verdict doesn't block.
                let completes = verdict != "REQUEST_CHANGES";
                if verdict == "REQUEST_CHANGES" && completes {
                    fail("invariant (h): the run completed under a blocking review".into());
                }
                if want("verdict").as_str() != Some(verdict) {
                    fail(format!("verdict {verdict}, want {}", want("verdict")));
                }
                if want("closes_own_pr").as_bool() != Some(override_found) {
                    fail(format!(
                        "closes_own_pr = {override_found}, want {}",
                        want("closes_own_pr")
                    ));
                }
            }
            other => fail(format!("unknown run case kind {other}")),
        }
    }
    assert!(
        failures.is_empty(),
        "run eval failures:\n{}",
        failures.join("\n")
    );
}

/// Live layer for the run cases: the model reads each recorded ticket
/// comment, and its intent must be the case's.
#[tokio::test]
#[ignore]
async fn review_eval_live_ticket_comments() {
    let (client, model) = eval_client().await;
    let mut usage = crate::models::TokenUsage::default();
    let mut failures = vec![];
    for (file, case) in run_cases() {
        if case.kind != "jira_comment" {
            continue;
        }
        let msg = crate::models::JiraCommentMessage {
            team_id: String::new(),
            installation_id: 0,
            ticket_id: "ABC-1".into(),
            title: "A class is missing from the site schedule".into(),
            run_id: String::new(),
            run_status: case.run_status.clone(),
            pr_number: 0,
            repo_owner: String::new(),
            repo_name: String::new(),
            author: "a person".into(),
            comment: case.comment.clone(),
            context_hash: String::new(),
            action: None,
        };
        let v = crate::agent::anthropic::converse_json(
            &client,
            &model,
            super::jira_comment::SYSTEM,
            &super::jira_comment::prompt(&msg),
            &super::jira_comment::schema(),
            true,
            "low",
            512,
            &mut usage,
        )
        .await;
        let want = common::ticket_state::Intent::parse(&case.intent);
        match v.map(|v| common::ticket_state::Intent::parse(v["intent"].as_str().unwrap_or(""))) {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{file}: read as {got:?}, want {want:?}")),
            Err(e) => failures.push(format!("{file}: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "live run eval failures:\n{}",
        failures.join("\n")
    );
}
