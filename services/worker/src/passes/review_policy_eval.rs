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
//! The severity cases: an installer's documented success code thrown on in
//! a startup script, a 204/304 failing a deploy check, a cursor defect traced
//! through the code, a data-confirmed defect talked down on impact, a PR that
//! misses its own goal (with a recorded severity second-pass answer), a
//! written team rule broken, and the notes that must stay notes (speculation,
//! inferred style); plus the graded audit findings in `misgraded_findings`.
//!
//! Adding a case: a JSON file in `review_cases/policy/` with `steps` and
//! `expect`. A review step carries the model's answer (`model`) and, when
//! the severity second pass should answer, the judge's (`judge`); a
//! `team_rules` step sets the team's written rules for what follows.
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
    /// The team's written rules (repository and team instructions).
    TeamRules(String),
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
    /// What the severity second pass answered (`{"checks": [...]}`), if it
    /// ran; without it the second pass is taken as unreachable (fail-open).
    #[serde(default)]
    judge: Option<Value>,
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
    team_rules: String,
    comments: Vec<Value>,
    /// Conditional-write claims held.
    claims: HashSet<String>,
    /// Stored review records, newest first.
    records: Vec<ReviewRecord>,
    formal: Vec<Formal>,
    threads: Vec<Thread>,
    lessons: Vec<String>,
    skipped: usize,
    /// Asks waiting for a running review of their commit.
    deferred: Vec<ReviewStep>,
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
                // Another review of the commit is running: the ask waits for
                // it and answers after (`deferred_ask`).
                self.skipped += 1;
                self.deferred.push(step.clone());
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
        super::review_severity::ground(&mut findings, &self.team_rules);
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
        // The severity second pass, with the judge's recorded answer.
        let candidates = super::review_severity::second_pass_candidates(&findings);
        if let Some(answers) = job
            .step
            .judge
            .as_ref()
            .and_then(|j| super::review_severity::parse_judge(&j.to_string()))
        {
            super::review_severity::apply_judge(&mut findings, &candidates, &answers);
        }
        super::review_severity::finalize(&mut findings);

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
            policy::trigger_kind(&job.step.trigger) == policy::TriggerKind::Explicit,
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
        // Invariant (c): a person's ask always gets a visible verdict, and so
        // does a commit a person pushed.
        if !matches!(decision, Decision::Post { .. }) {
            if explicit {
                self.violations.push(format!(
                    "a person's {} ask on {} got no visible verdict",
                    job.step.trigger, job.head
                ));
            }
            if prev.as_ref().is_some_and(|p| p.head_sha != job.head)
                || (prev.is_none() && !self.formal.iter().any(|f| f.head == job.head))
            {
                self.violations.push(format!(
                    "the pushed commit {} got no visible review",
                    job.head
                ));
            }
        }
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
                Step::TeamRules(text) => sim.team_rules = text.clone(),
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
                    // The asks that waited run once the running review is done.
                    for r in std::mem::take(&mut sim.deferred) {
                        if let Some(job) = sim.begin(&r, group + 1000) {
                            sim.finish(job);
                        }
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
    // A high-rated defect the PR already tracks holds the merge only
    // without rule 3.
    let tracked = Finding {
        file: "a.rs".into(),
        line: 1,
        severity: "high".into(),
        category: "bug".into(),
        evidence: Some("verified-defect".into()),
        tracked: Some(true),
        ..Default::default()
    };
    assert_eq!(
        review_agent::decide_verdict(std::slice::from_ref(&tracked), 0, &[], false),
        "REQUEST_CHANGES"
    );
    let mut fs = vec![tracked];
    policy::apply_blocking_policy(&mut fs, &[]);
    assert_eq!(review_agent::decide_verdict(&fs, 0, &[], false), "APPROVE");
    // An installer's documented success code thrown on in a startup script
    // is a note only without the grounding step.
    let installer: Finding = serde_json::from_value(serde_json::json!({
        "file": "modules/host/main.tf", "line": 4, "severity": "medium", "category": "hardening",
        "evidence": "unverified-concern",
        "title": "msiexec 3010 would be logged as a failure",
        "body": "The startup script throws on any non-zero code."
    }))
    .unwrap();
    assert_eq!(
        review_agent::decide_verdict(std::slice::from_ref(&installer), 0, &[], false),
        "APPROVE"
    );
    let mut fs = vec![installer];
    super::review_severity::ground(&mut fs, "");
    assert_eq!(
        review_agent::decide_verdict(&fs, 0, &[], false),
        "REQUEST_CHANGES"
    );
}

// Graded findings from an audit of the reviewer's non-blocking findings over
// 60 days, replayed through `Sim` like the JSON cases. Each positive case is
// a finding the reviewer posted as a note that the rubric says should have
// held the merge, with the labels the model gave it (severity, category,
// evidence) and the hedge that talked it down. Each negative case is a note
// that was rightly a note and must stay one, so the rubric can't simply
// block everything. All text is anonymized and rewritten with generic code.
// To add one, append a `Graded` to `should_have_blocked` or `true_notes`.

/// Notes the reviewer posted that should have held the merge, and notes that
/// rightly didn't.
#[cfg(test)]
mod misgraded_findings {
    use super::*;
    use serde_json::json;

    /// The rubric line a case rests on.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Rule {
        /// Defect shown by cited code, tests/CI, a data query, or documented
        /// tool/API behaviour.
        VerifiedDefect,
        /// Lost or stale writes, cursor or watermark errors, duplicates,
        /// wrong results on a production path.
        DataIntegrity,
        /// A one-shot production path with no automatic retry (startup or
        /// provisioning scripts, migrations, deploy steps), even if rare.
        OneShot,
        /// The PR doesn't achieve its own stated goal.
        MissesGoal,
        /// Breaks the team's or repository's written rules.
        BrokenTeamRules,
        Security,
        /// Negative: inferred style.
        Style,
        /// Negative: an optional improvement to correct code.
        Optional,
        /// Negative: depends on external state nobody can check here.
        ExternalUnknown,
    }

    impl Rule {
        fn blocks(self) -> bool {
            !matches!(self, Rule::Style | Rule::Optional | Rule::ExternalUnknown)
        }
    }

    struct Graded {
        name: &'static str,
        rule: Rule,
        /// The finding as the model answered it.
        finding: Value,
    }

    fn finding(
        file: &str,
        line: u64,
        severity: &str,
        category: &str,
        evidence: &str,
        title: &str,
        body: &str,
    ) -> Value {
        json!({
            "file": file, "line": line, "severity": severity, "category": category,
            "evidence": evidence, "title": title, "body": body,
        })
    }

    /// One push, one automatic review whose model answer is the single
    /// finding; a positive must request changes with no notes left, a
    /// negative must approve with the finding as its one note.
    fn case(g: &Graded) -> Case {
        let (event, notes) = if g.rule.blocks() {
            ("REQUEST_CHANGES", 0)
        } else {
            ("APPROVE", 1)
        };
        serde_json::from_value(json!({
            "description": g.name,
            "steps": [
                {"push": {"head": "ab12cd3"}},
                {"review": {"trigger": "synchronize", "dedup_key": "head#ab12cd3",
                    "model": {"verdict": "APPROVE", "summary": "x", "findings": [g.finding]}}}
            ],
            "expect": {"formal": [["ab12cd3", event]], "last_notes": notes}
        }))
        .expect("a graded case is a Case")
    }

    fn should_have_blocked() -> Vec<Graded> {
        vec![
            Graded {
                name: "one-shot startup script treats the installer's documented success-with-reboot code as failure",
                rule: Rule::OneShot,
                finding: finding(
                    "modules/host/user_data.ps1", 117, "low", "correctness", "verified-defect",
                    "Installer's success-with-reboot exit code would be logged as a failure",
                    "The installer is run with the no-restart switch, and its documented exit codes \
                     include a success-with-reboot-required code next to 0. The new check \
                     `if ($p.ExitCode -ne 0) { throw }` throws on any non-zero code, so that code \
                     stops the script before the next install step. A fresh install usually returns \
                     0, so this may never happen. If it does, the log shows FAILED for a working \
                     install and someone has to finish it by hand. You could accept the reboot code \
                     as success too.",
                ),
            },
            Graded {
                name: "a kept updated_at hides other column writes from both incremental cursors",
                rule: Rule::DataIntegrity,
                finding: finding(
                    "jobs/common/upsert.py", 1204, "medium", "bug", "verified-defect",
                    "Kept updated_at hides the older record's other writes from both incremental cursors",
                    "When keep_text is true this UPDATE still writes the older record's values to \
                     parent_id, status and image_url, but updated_at stays the same. Two cursors \
                     read that column: the watermark is MAX(updated_at) over the table \
                     (sync_utils.py L357-359), and the search sync selects \
                     `GREATEST(g.updated_at, MAX(c.updated_at))`. So in an incremental run the edit \
                     lands in the database but reaches neither consumer until the nightly rebuild, \
                     and the watermark never moves past it, so the record is fetched again every \
                     run. One option is to keep only the text column and still move updated_at \
                     forward.",
                ),
            },
            Graded {
                name: "ties still flip between runs, the flip-flop the PR set out to remove",
                rule: Rule::MissesGoal,
                finding: finding(
                    "jobs/common/upsert.py", 1163, "low", "bug", "verified-defect",
                    "Ties on the latest start date with different text still flip between runs",
                    "Equal start dates never keep the text. When two children of a group share the \
                     latest start date and differ, a full sync ends on the higher \
                     (updated_at, id), but an incremental run lets whichever tied child was fetched \
                     write. That is the flip-flop this PR removes for the general case. A query on \
                     the source table found groups in exactly this state. Most look old, so the \
                     impact is likely small. Breaking ties with the same (updated_at, id) order the \
                     full sync uses would make both modes agree.",
                ),
            },
            Graded {
                name: "skipped records with no WARNING and no count, against a written team rule",
                rule: Rule::BrokenTeamRules,
                finding: finding(
                    "jobs/sync_pipeline.py", 1328, "medium", "observability", "verified-defect",
                    "Missing parent row and undated children are skipped with no warning or count",
                    "The team rules ask for a WARNING with the record id, plus a count in the run \
                     summary, when data doesn't have the expected shape. Here a missing parent row \
                     turns into `[]`, and a child with no start date is ignored without a word: \
                     `if not start: continue`. The description lists this under 'Not done'. A \
                     deduplicated WARNING per id and a count next to the summary line would cover it.",
                ),
            },
            Graded {
                name: "check-then-act guard still lets concurrent webhooks create duplicates",
                rule: Rule::DataIntegrity,
                finding: finding(
                    "src/payments/agreement.service.ts", 88, "medium", "concurrency", "verified-defect",
                    "Guard is check-then-act with a wide window; concurrent webhooks can still double-create",
                    "The `created:` key is read at the top of the method but only written after the \
                     remote create call and finalization complete, seconds later. The provider can \
                     deliver webhooks in parallel and nothing in the webhook path serializes by \
                     basket. Two near-simultaneous webhooks can both pass the check before either \
                     sets the guard, so two agreements are created. This addresses the dominant \
                     pattern (retries minutes apart) but doesn't eliminate the class. Consider \
                     claiming the guard atomically (set-if-absent) before the remote call.",
                ),
            },
            Graded {
                name: "a late event after the timeout overwrites the error and activates the abandoned record",
                rule: Rule::DataIntegrity,
                finding: finding(
                    "src/payments/agreement.service.ts", 214, "medium", "bug", "verified-defect",
                    "Late confirmation after the timeout undoes the error and activates the abandoned agreement",
                    "When the hold expires, `expireHold` writes `{status: 'error'}` and deletes the \
                     hold, leaving nothing the confirmation handler checks. A confirmation that \
                     arrives later still runs `handleConfirmation`, which sets the session to `good` \
                     without reading its status and activates the old agreement. The user was told \
                     to try again, so a retry creates a second live agreement. Today no late \
                     confirmations arrive, so this is latent, but the description says the sender \
                     is being fixed separately.",
                ),
            },
            Graded {
                name: "the skip the PR adds is never reached, so the tests still fail",
                rule: Rule::MissesGoal,
                finding: finding(
                    "e2e/listing.spec.ts", 40, "blocking", "tests", "unverified-concern",
                    "openAnyResult still times out when nothing is open, so the skip never runs",
                    "`openAnyResult` (pages/listing.ts:293-301) waits for a details button, an empty \
                     heading, or an invalid-filters message. When nothing is open, the block \
                     returns `<OffSeason>` (block.tsx:418) before fetching, which renders none of \
                     those. The `toBeVisible()` inside `openAnyResult` times out before `test.skip` \
                     runs, so all six tests still fail, now in a different step.",
                ),
            },
            Graded {
                name: "CI is red because of the change, shown by the failed check run",
                rule: Rule::VerifiedDefect,
                finding: finding(
                    "requirements.txt", 1, "blocking", "ci", "unverified-concern",
                    "CI can't find the constraints file at the runtime path, and the Tests check is red",
                    "The test workflow installs `-r requirements.txt` from the checkout. Line 1 now \
                     references `/opt/runtime/constraints.txt`, a path that only exists in the \
                     hosted runtime, so the install fails before any test runs. That matches the \
                     failed Tests run on this head, while the PRs before it were green. The deploy \
                     workflow calls the test workflow first, so on main this blocks every deploy.",
                ),
            },
            Graded {
                name: "existing tests still assert the old copy and will fail",
                rule: Rule::VerifiedDefect,
                finding: finding(
                    "src/screens/confirm/index.tsx", 52, "high", "tests", "unverified-concern",
                    "Confirmation tests still look for the old copy",
                    "__tests__/screens/Confirm.test.tsx calls getByText('Pay now') (line ~52) and \
                     getByText('Pay later') (line ~70). This PR renders 'Pay and finish' and 'Add to \
                     next bill', so none of those lookups match and three tests fail. The \
                     `queryByText(/Pay now/)` null checks still pass but no longer prove anything.",
                ),
            },
            Graded {
                name: "transaction-start timestamps let committed rows land behind the cursor",
                rule: Rule::DataIntegrity,
                finding: finding(
                    "jobs/incremental_sync.py", 77, "high", "bug", "unverified-concern",
                    "Rows from a long write transaction can still land behind the write-time cursor",
                    "synced_at is set with CURRENT_TIMESTAMP, the transaction start, not the \
                     commit. The batch writer upserts every row with one execute_values and commits \
                     at the end (writer.py:4354-4355), while a per-row updater writes at the same \
                     time with fresh stamps. When a batch starts at T and commits at T+20s, a run at \
                     T+10s moves the cursor to T+9s, and the batch rows stamped T become visible \
                     behind it and are never synced.",
                ),
            },
            Graded {
                name: "a network failure is recorded as sent because the client returns errors instead of throwing",
                rule: Rule::VerifiedDefect,
                finding: finding(
                    "src/mail/notify.service.ts", 61, "medium", "correctness", "verified-defect",
                    "A mail-provider network failure is recorded as sent, with no warning",
                    "`MailClient.send` catches every error and returns it (mail-client.ts lines \
                     95-98). An API rejection has `.error` and maps to `send_failed`, but a network \
                     or timeout failure is a plain `Error` with no `.error`, so this line returns \
                     `sent`. The event then says the recipient was emailed when they weren't, and the \
                     team's rules say unexpected shapes must not fall back silently.",
                ),
            },
            Graded {
                name: "an unauthenticated action now answers questions about another account",
                rule: Rule::Security,
                finding: finding(
                    "src/actions/createBasket.ts", 30, "medium", "security", "verified-defect",
                    "Unauthenticated server action now reveals whether any account id has a subscription",
                    "`accountId` comes straight from the client and nothing checks the caller is \
                     that account. The response now differs (`existing: true` vs a new basket), so \
                     anyone can learn whether an account holds a subscription, and ids are \
                     sequential so they're easy to enumerate. The leak is small (status only), but \
                     it's a new way to probe another account's data.",
                ),
            },
            Graded {
                name: "a destroy that only applies if a resource stays empty can leave a partial apply",
                rule: Rule::OneShot,
                finding: finding(
                    "infra/main.tf", 12, "low", "deploy", "verified-defect",
                    "Emptying the registry can be undone by a push before the apply",
                    "The removed module sets `force_delete = false` on the registry, so the apply \
                     only passes if it is still empty when the merge applies. Another repository's \
                     deploy workflow pushes to it, and the CI role trusts that repository \
                     (iam.tf:21). A push between the manual empty and the apply fails the apply \
                     after the function and API are already destroyed, leaving a partly applied \
                     state. Heads-up: disable that workflow first.",
                ),
            },
            Graded {
                name: "a timeout while reading the body escapes the handler the PR adds for it",
                rule: Rule::VerifiedDefect,
                finding: finding(
                    "app/api/checkout/route.ts", 58, "medium", "error-handling", "verified-defect",
                    "A timeout while reading the response body escapes the 504 handling and becomes a 500",
                    "`AbortSignal.timeout(TIMEOUT_MS)` also covers reading the body, but \
                     `await response.text()` sits outside the try/catch. If headers arrive just \
                     before the deadline, `text()` throws a TimeoutError nothing catches: the route \
                     returns a generic 500, the client re-enables Pay, and the upstream may already \
                     have charged. The PR's 504 path exists for exactly this case.",
                ),
            },
            Graded {
                name: "a new gate passes green when it collected nothing",
                rule: Rule::MissesGoal,
                finding: finding(
                    "scripts/memory-verdict.ts", 20, "medium", "bug", "verified-defect",
                    "A run with zero samples passes silently",
                    "`sampleMemory` swallows every error and null reading. If the platform call is \
                     unsupported on a device, no sample is written, the verdict prints 'no samples \
                     collected, nothing to judge' and the suite exits 0. The gate the PR adds \
                     silently doesn't run, and the description says the collector has not run end \
                     to end yet, so this is the most likely way it fails.",
                ),
            },
            Graded {
                name: "a shipped agent instruction contradicts the repository's written test rule",
                rule: Rule::BrokenTeamRules,
                finding: finding(
                    "e2e/agents/test-healer.md", 1, "low", "docs", "verified-defect",
                    "Healer agent is told to fixme failing tests, which the repo's rules forbid for payment tests",
                    "This agent file says that when an error persists it should mark the test \
                     `test.fixme()` and do whatever passes the test. The repository's own rules \
                     (e2e/RULES.md) say payment tests are never skipped, fixme'd or loosened. An \
                     agent invoked directly just follows its own prompt.",
                ),
            },
            Graded {
                name: "a one-shot batch script marks every remaining batch terminal on one lasting rejection",
                rule: Rule::OneShot,
                finding: finding(
                    "scripts/send-verification.ts", 140, "medium", "hardening", "verified-defect",
                    "One lasting rejection can mark every remaining batch as failed for good",
                    "A definitive rejection is now recorded as `failed`, and `failed` is in \
                     TERMINAL_STATUSES. If the rejection lasts (a revoked token, a template mismatch), \
                     the loop marks every remaining batch failed. Nothing was sent, but a rerun \
                     skips them and the only way back is hand-editing the state file. Two cheap \
                     guards: stop after a few rejections in a row, and drop `failed` from the \
                     terminal set.",
                ),
            },
            Graded {
                name: "an exit check passes on a missing element, so the step goes green without proving the exit",
                rule: Rule::MissesGoal,
                finding: finding(
                    "e2e/steps/exit.steps.ts", 33, "medium", "tests", "verified-defect",
                    "Exit treats the missing row as success, so a late dialog can still make it pass falsely",
                    "Each iteration dismisses, taps back if visible, then checks `remaining` is not \
                     displayed. If a native dialog appears just after the dismiss returns, the \
                     screen behind it isn't in the hierarchy: `back.isDisplayed()` and \
                     `remaining.isDisplayed()` are both false and the loop ends with the dialog up. \
                     That's the exact timing this PR describes; it passes today only because a \
                     global After hook unwinds anyway.",
                ),
            },
        ]
    }

    fn true_notes() -> Vec<Graded> {
        vec![
            Graded {
                name: "a comment that went stale",
                rule: Rule::Style,
                finding: finding(
                    "infra/rules.tf", 932, "low", "docs", "verified-defect",
                    "Comment on the exclusion list is now stale",
                    "The comment below this hunk still describes the list as one group, but it is \
                     now split in two and the exemption covers both. Update the comment and drop the \
                     old count.",
                ),
            },
            Graded {
                name: "a doc comment left above the wrong function",
                rule: Rule::Style,
                finding: finding(
                    "src/analytics/identify.ts", 14, "nit", "style", "verified-defect",
                    "identify()'s doc comment now sits above refuse()",
                    "refuse() was inserted between identify()'s doc comment and identify(), so \
                     editors show identify's text on refuse. Move refuse() above the comment.",
                ),
            },
            Graded {
                name: "a naming pattern differs, with no defect",
                rule: Rule::Style,
                finding: finding(
                    "infra/secrets.tf", 40, "low", "naming", "verified-defect",
                    "User name breaks the `<vendor>_user` pattern without a stated reason",
                    "Every other secret in this module sets the user to `<vendor>_user`; this one \
                     uses a plain name. If that's intended, a note in the description is enough.",
                ),
            },
            Graded {
                name: "a shared constant not used everywhere it could be",
                rule: Rule::Optional,
                finding: finding(
                    "jobs/common/targets.py", 9, "nit", "maintainability", "verified-defect",
                    "CONFLICT_TARGET isn't used by the pipelines",
                    "Two pipelines repeat the string inline and only the test reads the constant. \
                     You could interpolate the constant in both, or drop it.",
                ),
            },
            Graded {
                name: "telemetry attribution of a pre-check, not a correctness issue",
                rule: Rule::Optional,
                finding: finding(
                    "src/flows/confirm.ts", 77, "low", "observability", "verified-defect",
                    "Pre-check failures are recorded as update failures",
                    "When the pre-check fails with a 5xx, the error is tracked against the update \
                     endpoint although no update was sent. This is not a correctness issue; a step \
                     attribute would make the dashboards clearer.",
                ),
            },
            Graded {
                name: "hardening of a script whose verification step already catches the failure, rated high",
                rule: Rule::Optional,
                finding: finding(
                    "modules/host/user_data.ps1", 20, "high", "hardening", "verified-defect",
                    "Download failures don't show up as a clear exit code in the log",
                    "The script keeps the default error preference, so a failed download continues \
                     and the next call fails with 'not recognized'. Verification step 2 still \
                     catches it because the line wouldn't read 0. If you'd like the log to name the \
                     failing step, set `$ErrorActionPreference = 'Stop'` at the top.",
                ),
            },
            Graded {
                name: "whether a vendor API accepts an option, failing loudly if not",
                rule: Rule::ExternalUnknown,
                finding: finding(
                    "scripts/alerts.sh", 31, "medium", "question", "question",
                    "Is a sliding window accepted with the event-timer aggregation?",
                    "I couldn't confirm the alerting API accepts a sliding window with this \
                     aggregation, and the description says the calls haven't run yet. If it's \
                     refused, the script fails loudly, so there's no harm.",
                ),
            },
            Graded {
                name: "an additive permission that may need a wider resource, unknowable from the repo",
                rule: Rule::ExternalUnknown,
                finding: finding(
                    "infra/iam.tf", 55, "low", "permissions", "unverified-concern",
                    "Does the list call need a wildcard resource?",
                    "A list call without a prefix is often authorised only against a wildcard. If \
                     the forwarder calls it unfiltered, these ARNs may not cover it. The policy only \
                     adds permissions, so it can't break anything that works today.",
                ),
            },
        ]
    }

    fn replay(cases: Vec<Graded>) -> Vec<String> {
        let mut failures = vec![];
        for g in cases {
            let c = case(&g);
            let sim = Sim::run(&c);
            let got: Vec<(String, String)> = sim
                .formal
                .iter()
                .map(|f| (f.head.clone(), f.event.to_string()))
                .collect();
            let mut why = sim.violations.clone();
            if got != c.expect.formal {
                why.push(format!("formal {got:?}, want {:?}", c.expect.formal));
            }
            let notes = sim.formal.last().map(|f| f.notes).unwrap_or(0);
            if Some(notes) != c.expect.last_notes {
                why.push(format!("{notes} notes, want {:?}", c.expect.last_notes));
            }
            if !why.is_empty() {
                failures.push(format!("[{:?}] {}: {}", g.rule, g.name, why.join("; ")));
            }
        }
        failures
    }

    #[test]
    fn misgraded_findings_hold_the_merge() {
        let cases = should_have_blocked();
        assert!(cases.iter().all(|g| g.rule.blocks()));
        let failures = replay(cases);
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[test]
    fn true_notes_stay_notes() {
        let cases = true_notes();
        assert!(cases.iter().all(|g| !g.rule.blocks()));
        let failures = replay(cases);
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
