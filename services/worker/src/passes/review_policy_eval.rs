//! Offline evaluation of the verdict rules (`review_policy`): review
//! sequences from the production audit, anonymized as fixtures in
//! `review_cases/policy/`, replayed through the same steps `review_pr::run_once`
//! takes after the model answers (start check, head claim, blocking policy,
//! late blockers, verdict, stability, the one verdict function, the verdict
//! claim). No model, no network: every review step carries the model output
//! the audit saw.
//!
//! The sequences: the same head triggered twice (a push and a re-review ask
//! together, two copies of one push), approve then a flip on the same commit,
//! a blocker first raised in round 3, a failure that comes from the base
//! branch, and nits and hardening rated high.
//!
//! Every case also checks two invariants:
//!   (a) a head never gets two formal reviews from one trigger kind at once,
//!       and an automatic trigger never reviews a reviewed head again;
//!   (b) a verdict on a commit never flips without new information, and a
//!       flip always says why.

use super::review_agent::{self, Finding, ReviewOutput};
use super::review_policy as policy;
use super::review_sweep::ReviewRecord;
use super::review_verdict::{decide, Context, Decision, Input, StandingReview};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Deserialize)]
struct Case {
    #[allow(dead_code)]
    description: String,
    steps: Vec<Step>,
    expect: Expect,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Step {
    /// A new head; `changed` are the right-side lines it changed against the
    /// head before it. `labeled`: the gateway takes the per-commit claim and
    /// enqueues a review.
    Push {
        head: String,
        #[serde(default)]
        changed: HashMap<String, Vec<u64>>,
    },
    /// Checks failing on the base branch from now on.
    BaseFailing(Vec<String>),
    /// A person writes on the PR.
    Comment { by: String, text: String },
    /// Claims older than their window are gone.
    ClaimsExpire {},
    /// One review job, start to finish.
    Review(ReviewStep),
    /// Review jobs that start before any of them finishes.
    Together(Vec<ReviewStep>),
}

#[derive(Deserialize, Clone)]
struct ReviewStep {
    trigger: String,
    #[serde(default)]
    dedup_key: String,
    /// What the model answered (a `ReviewOutput`); absent when the job is
    /// expected to stop before the model.
    #[serde(default)]
    model: Option<Value>,
}

#[derive(Deserialize)]
struct Expect {
    /// Every formal review posted, in order: (head, event).
    formal: Vec<(String, String)>,
    #[serde(default)]
    skipped: usize,
    /// Lessons the reviewer kept (ids).
    #[serde(default)]
    lessons: Vec<String>,
    /// (index of a formal review, text its body must contain).
    #[serde(default)]
    body_contains: Vec<(usize, String)>,
    /// Notes (findings that don't hold the merge) in the last formal review.
    #[serde(default)]
    last_notes: Option<usize>,
}

struct Formal {
    /// The jobs that ran together share a group.
    group: usize,
    head: String,
    event: &'static str,
    body: String,
    notes: usize,
}

struct Thread {
    file: String,
    line: u64,
    holds: bool,
    open: bool,
}

/// A review job between its start checks and its verdict.
struct Job {
    step: ReviewStep,
    head: String,
    /// Records as the job read them when it started.
    records: Vec<ReviewRecord>,
    group: usize,
}

#[derive(Default)]
struct Sim {
    head: String,
    /// Heads in push order, with the lines each changed.
    pushes: Vec<(String, HashMap<String, HashSet<u64>>)>,
    base_failing: Vec<String>,
    comments: Vec<Value>,
    /// Conditional-write claims held.
    claims: HashSet<String>,
    /// Stored review records, newest first.
    records: Vec<ReviewRecord>,
    formal: Vec<Formal>,
    threads: Vec<Thread>,
    lessons: Vec<String>,
    skipped: usize,
    minute: i64,
    violations: Vec<String>,
}

fn ts(minute: i64) -> String {
    (chrono::DateTime::<chrono::Utc>::from_timestamp(1_800_000_000, 0).unwrap()
        + chrono::Duration::minutes(minute))
    .to_rfc3339()
}

impl Sim {
    fn tick(&mut self) -> String {
        self.minute += 1;
        ts(self.minute)
    }

    fn standing(&self) -> Option<StandingReview> {
        self.formal.last().map(|f| StandingReview {
            verdict: f.event.to_string(),
            failed: false,
            head_sha: f.head.clone(),
        })
    }

    /// Lines changed between `from` and the current head.
    fn changed_since(&self, from: &str) -> Option<HashMap<String, HashSet<u64>>> {
        let at = self.pushes.iter().position(|(h, _)| h == from)?;
        let mut out: HashMap<String, HashSet<u64>> = HashMap::new();
        for (_, changed) in &self.pushes[at + 1..] {
            for (file, lines) in changed {
                out.entry(file.clone()).or_default().extend(lines);
            }
        }
        Some(out)
    }

    /// The checks `run_once` makes before any model work. None: the job
    /// stopped there.
    fn begin(&mut self, step: &ReviewStep, group: usize) -> Option<Job> {
        let head = self.head.clone();
        if let policy::Start::ReturnExisting { .. } =
            policy::start(&step.trigger, &head, &self.records)
        {
            self.skipped += 1;
            return None;
        }
        if policy::takes_head_claim(&step.trigger) {
            let sk = policy::head_claim_sk("acme", "app", 7, &head);
            if !self.claims.insert(sk) && policy::review_of_head(&self.records, &head).is_none() {
                self.skipped += 1;
                return None;
            }
        }
        Some(Job {
            step: step.clone(),
            head,
            records: self.records.clone(),
            group,
        })
    }

    /// The rest of `run_once`, from the model's answer to the post.
    fn finish(&mut self, job: Job) {
        let Some(model) = job.step.model.clone() else {
            self.violations.push(format!(
                "a {} review of {} ran without a model answer in the case",
                job.step.trigger, job.head
            ));
            return;
        };
        let out: ReviewOutput = serde_json::from_value(model).expect("model is a ReviewOutput");
        let mut findings: Vec<Finding> = out.findings.clone();
        review_agent::enforce_source_data_override(&mut findings);
        review_agent::enforce_manual_checks_are_notes(&mut findings);
        policy::apply_blocking_policy(&mut findings, &self.base_failing);

        // Earlier threads the model reports settled close.
        let mut open: Vec<usize> = (0..self.threads.len())
            .filter(|&i| self.threads[i].open)
            .collect();
        for p in out.prior.iter().filter(|p| p.is_settled()) {
            if let Some(&i) = open.get(p.index) {
                self.threads[i].open = false;
            }
        }
        open.retain(|&i| self.threads[i].open);
        let holding = open.iter().filter(|&&i| self.threads[i].holds).count();

        let previous_at = job
            .records
            .first()
            .map(|r| r.created_at.clone())
            .unwrap_or_default();
        let spoke = policy::person_spoke_since(&self.comments, &previous_at);
        let round = policy::round(&job.records, &job.head);
        let changed = round
            .previous_head
            .as_deref()
            .and_then(|p| self.changed_since(p));
        let earlier: Vec<(String, u64)> = self
            .threads
            .iter()
            .map(|t| (t.file.clone(), t.line))
            .collect();
        let late = policy::hold_late_blockers(
            &mut findings,
            &policy::UpFront {
                round: &round,
                changed_since_previous: changed.as_ref(),
                earlier_threads: &earlier,
                person_spoke: spoke.is_some(),
            },
        );
        if late.self_lesson {
            let id = policy::RAISE_EARLY_LESSON_ID.to_string();
            if !self.lessons.contains(&id) {
                self.lessons.push(id);
            }
        }

        let unconfirmed = review_agent::unconfirmed_external_facts("", &out.unconfirmed_facts);
        let verdict = review_agent::decide_verdict(&findings, holding, &unconfirmed, false);
        let same_head = policy::review_of_head(&job.records, &job.head).cloned();
        let new_info = same_head.as_ref().and_then(|prev| {
            policy::person_spoke_since(&self.comments, &prev.created_at)
                .map(|who| format!("what {who} wrote since my last review"))
        });
        let stable = policy::stabilize(
            same_head.as_ref(),
            verdict,
            &mut findings,
            new_info.as_deref(),
            &out.change_of_mind,
        );
        // Invariant (b).
        if let Some(prev) = &same_head {
            let before = super::review_verdict::binary(&prev.verdict);
            if before != stable.verdict && new_info.is_none() {
                self.violations.push(format!(
                    "the verdict on {} flipped from {before} to {} with nothing new",
                    job.head, stable.verdict
                ));
            }
            if before != stable.verdict
                && !stable
                    .note
                    .as_deref()
                    .unwrap_or("")
                    .contains("Changed my verdict")
            {
                self.violations.push(format!(
                    "the verdict on {} changed without a reason",
                    job.head
                ));
            }
        }
        let verdict = stable.verdict;

        let prev = self.records.first().cloned();
        let explicit = policy::trigger_kind(&job.step.trigger) == policy::TriggerKind::Explicit;
        let has_news = prev.as_ref().map(|p| p.verdict.as_str()) != Some(verdict)
            || !findings.is_empty()
            || prev.as_ref().is_some_and(|p| p.head_sha != job.head);
        let decision = decide(
            Input::Review {
                verdict,
                has_news,
                explicit,
                already_answered: false,
            },
            &Context {
                standing: self.standing(),
                self_authored: false,
            },
        );
        let sk = policy::verdict_claim_sk(
            "acme",
            "app",
            7,
            &job.head,
            &job.step.trigger,
            &job.step.dedup_key,
        );
        let decision = match decision {
            Decision::Post { .. } if !self.claims.insert(sk) => Decision::Nothing,
            d => d,
        };
        let at = self.tick();
        if let Decision::Post { event, .. } = decision {
            let mut body = String::new();
            if let Some(n) = &stable.note {
                body.push_str(n);
                body.push('\n');
            }
            for f in findings.iter().chain(late.held.iter()) {
                body.push_str(&format!("- [{}] {} {}\n", f.severity, f.title, f.body));
            }
            // Invariant (a): jobs that ran together post one verdict per
            // head, and an automatic trigger never reviews a reviewed head.
            if self
                .formal
                .iter()
                .any(|f| f.head == job.head && f.group == job.group)
            {
                self.violations.push(format!(
                    "{} got two verdicts from jobs that ran together",
                    job.head
                ));
            }
            if !explicit && policy::review_of_head(&job.records, &job.head).is_some() {
                self.violations.push(format!(
                    "an automatic {} trigger reviewed {} again",
                    job.step.trigger, job.head
                ));
            }
            self.formal.push(Formal {
                group: job.group,
                head: job.head.clone(),
                event,
                body,
                notes: findings.iter().filter(|f| !f.holds_merge()).count() + late.held.len(),
            });
            for f in &findings {
                self.threads.push(Thread {
                    file: f.file.clone(),
                    line: f.line,
                    holds: f.holds_merge(),
                    open: true,
                });
            }
        }
        self.records.insert(
            0,
            ReviewRecord {
                verdict: verdict.to_string(),
                posted_as: verdict.to_string(),
                head_sha: job.head.clone(),
                created_at: at,
                body: "review".into(),
            },
        );
    }

    fn run(case: &Case) -> Sim {
        let mut sim = Sim::default();
        for (group, step) in case.steps.iter().enumerate() {
            match step {
                Step::Push { head, changed } => {
                    sim.head = head.clone();
                    sim.pushes.push((
                        head.clone(),
                        changed
                            .iter()
                            .map(|(f, l)| (f.clone(), l.iter().copied().collect()))
                            .collect(),
                    ));
                    // The gateway's claim for the push review.
                    sim.claims
                        .insert(policy::head_claim_sk("acme", "app", 7, head));
                }
                Step::BaseFailing(names) => sim.base_failing = names.clone(),
                Step::Comment { by, text } => {
                    let at = sim.tick();
                    sim.comments.push(serde_json::json!({
                        "user": {"login": by, "type": "User"},
                        "body": text,
                        "created_at": at,
                    }));
                }
                Step::ClaimsExpire {} => {
                    sim.claims.retain(|c| !c.starts_with("REVIEWCLAIM#"));
                }
                Step::Review(r) => {
                    if let Some(job) = sim.begin(r, group) {
                        sim.finish(job);
                    }
                }
                Step::Together(rs) => {
                    let jobs: Vec<Job> = rs.iter().filter_map(|r| sim.begin(r, group)).collect();
                    for job in jobs {
                        sim.finish(job);
                    }
                }
            }
        }
        sim
    }
}

fn cases() -> Vec<(String, Case)> {
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/passes/review_cases/policy");
    let mut out = vec![];
    for entry in std::fs::read_dir(&dir).expect("review_cases/policy exists") {
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
    assert!(out.len() >= 5, "the policy cases are found");
    out
}

#[test]
fn review_policy_eval_replays_every_case() {
    let mut failures = vec![];
    for (file, case) in cases() {
        let sim = Sim::run(&case);
        let mut fail = |m: String| failures.push(format!("{file}: {m}"));
        for v in &sim.violations {
            fail(v.clone());
        }
        let got: Vec<(String, String)> = sim
            .formal
            .iter()
            .map(|f| (f.head.clone(), f.event.to_string()))
            .collect();
        if got != case.expect.formal {
            fail(format!(
                "formal reviews {got:?}, want {:?}",
                case.expect.formal
            ));
        }
        if sim.skipped != case.expect.skipped {
            fail(format!(
                "{} jobs skipped, want {}",
                sim.skipped, case.expect.skipped
            ));
        }
        if sim.lessons != case.expect.lessons {
            fail(format!(
                "lessons {:?}, want {:?}",
                sim.lessons, case.expect.lessons
            ));
        }
        for (i, text) in &case.expect.body_contains {
            match sim.formal.get(*i) {
                Some(f) if f.body.contains(text.as_str()) => {}
                Some(f) => fail(format!("formal #{i} lacks {text:?}: {}", f.body)),
                None => fail(format!("no formal review #{i}")),
            }
        }
        if let Some(n) = case.expect.last_notes {
            let got = sim.formal.last().map(|f| f.notes).unwrap_or(0);
            if got != n {
                fail(format!("last review has {got} notes, want {n}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The evaluation catches what it is for: replaying the audit's sequences
/// without the rules gives the bad outcomes back.
#[test]
fn without_the_rules_the_audit_sequences_misbehave() {
    // A high-rated style nit holds the merge only without rule 3.
    let nit = Finding {
        file: "a.rs".into(),
        line: 1,
        severity: "high".into(),
        category: "style".into(),
        evidence: Some("verified-defect".into()),
        ..Default::default()
    };
    assert_eq!(
        review_agent::decide_verdict(std::slice::from_ref(&nit), 0, &[], false),
        "REQUEST_CHANGES"
    );
    let mut fs = vec![nit];
    policy::apply_blocking_policy(&mut fs, &[]);
    assert_eq!(review_agent::decide_verdict(&fs, 0, &[], false), "APPROVE");
}
