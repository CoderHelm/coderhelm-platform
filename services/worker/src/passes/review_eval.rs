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
//! Live layer (`cargo test -p worker review_eval -- --ignored`, nightly): the
//! model reads each recorded comment and reply, and its classification must
//! land in the case's outcome class.

use super::review_agent::{self, Evidence, Finding, ReviewOutput, VerdictChange};
use super::review_pr::{
    failure_plan, reply_after_formal, retry_message, FailurePlan, FormalResult,
};
use super::review_sweep::{stuck, FailureNote, ReviewRecord};
use super::review_verdict::{decide, Context, Decision, Input, StandingReview};
use crate::models::ReviewMessage;
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
    steps: Vec<Step>,
    expect: Expect,
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
}

#[derive(Deserialize)]
struct FinalFormal {
    head: String,
    event: String,
}

/// An open CoderHelm thread on the PR.
struct Thread {
    holds: bool,
    question: bool,
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
    violations: Vec<String>,
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
            attempt: 0,
            wait_secs: 0,
            reconcile_note: None,
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
        let questions = self
            .threads
            .iter()
            .filter(|t| !t.holds && t.question)
            .count();
        let verdict = review_agent::decide_verdict(&findings, holding, questions, false);
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
        // New findings become open threads.
        for f in &findings {
            self.threads.push(Thread {
                holds: f.holds_merge(),
                question: f.evidence() == Evidence::Question,
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
                Some(event)
            }
            Decision::Nothing => None,
            Decision::Review => panic!("a finished review never asks for another"),
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
            }
        }
        sim
    }

    fn comment(&mut self, c: &CommentStep) {
        let key = format!("reply#{}", c.id);
        if !self.claim(&key) {
            return;
        }
        let kind = review_agent::parse_ask(&serde_json::json!({ "kind": c.ask_kind }));
        if kind.wants_formal_review() || self.self_authored {
            if let Some(r) = &c.review {
                self.finished_review(r, true);
            }
            return;
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

/// Live layer: the model reads each recorded comment and reply. Needs
/// ANTHROPIC_API_KEY (and optionally EVAL_MODEL); run nightly.
#[tokio::test]
#[ignore]
async fn review_eval_live_classification() {
    let Ok(key) = std::env::var("ANTHROPIC_API_KEY") else {
        panic!("ANTHROPIC_API_KEY is required for the live reviewer evals");
    };
    let model = std::env::var("EVAL_MODEL").unwrap_or_else(|_| "claude-haiku-4-5".into());
    let client =
        crate::agent::anthropic::AnthropicClient::new(key, common::usage::UsageMeter::memory("").0);
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
                &review_agent::ask_prompt(&c.text, "Changes requested.", None),
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
