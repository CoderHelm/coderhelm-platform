//! The worker's side of the stop judge (`common::stop_judge`).
//!
//! [`decide`] is the one call every costly decision makes: it puts the
//! picture to the strongest configured model at high effort, keeps the
//! decision with what it saw (`common::decision_log`) and returns the
//! [`Judged`] that the code doing the costly thing must hold:
//!
//! - writing `needs_input` on a run, and recording or posting a question for
//!   a person, happen only in functions that take a `Judged`;
//! - "changes requested" is posted only with a `Judged` that holds
//!   (`review_verdict::post`);
//! - a fix cycle beyond the first starts only with a [`CycleLicence`], and the
//!   one way to get one after the first cycle is a `Judged` that holds.
//!
//! The test at the bottom reads the worker's sources and fails when any of
//! these happens another way.

use common::decision_log::{self, Record, Subject};
use common::stop_judge::{self as judge, JudgeModel, JUDGMENT_EFFORT};
pub use common::stop_judge::{Budget, Evidence, Judged, Kind, Picture, Related, Thread};
use serde_json::Value;
use std::sync::Mutex;
use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

/// Output room for one answer: the schema's fields, a question with four
/// options, and the thinking high effort spends before it.
const MAX_TOKENS: i32 = 8_000;

/// The judge's model calls, through the worker's metered client.
struct ModelJudge<'a> {
    provider: &'a ModelProvider,
    model: String,
    usage: Mutex<TokenUsage>,
}

impl JudgeModel for ModelJudge<'_> {
    async fn answer(&self, system: &str, prompt: &str, schema: &Value) -> Result<Value, String> {
        let mut usage = TokenUsage::default();
        let out = crate::agent::anthropic::converse_json(
            &self.provider.client(),
            &self.model,
            system,
            prompt,
            schema,
            true,
            JUDGMENT_EFFORT,
            MAX_TOKENS,
            &mut usage,
        )
        .await
        .map_err(|e| e.to_string());
        if let Ok(mut u) = self.usage.lock() {
            u.merge(&usage);
        }
        out
    }
}

/// Puts a costly decision to the judge, keeps it, and returns it. Never
/// fails: an unusable answer falls back inside `common::stop_judge::judge`.
#[allow(clippy::too_many_arguments)]
pub async fn decide(
    state: &WorkerState,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    team_id: &str,
    subject: &Subject,
    head_sha: &str,
    picture: Picture,
) -> Judged {
    let model = ModelJudge {
        provider,
        model: provider.judgment_model_id().to_string(),
        usage: Mutex::new(TokenUsage::default()),
    };
    let mut picture = picture;
    if picture.overturned.is_empty() {
        picture.overturned = overturned(state, team_id, &picture).await;
    }
    if picture.memories.is_empty() {
        picture.memories = repo_memories(state, team_id, subject, &picture).await;
    }
    let picture = picture.with_ids();
    let judged = judge::judge(&model, picture.clone()).await;
    if let Ok(u) = model.usage.lock() {
        usage.merge(&u);
    }
    keep(state, team_id, subject, head_sha, &picture, &judged).await;
    judged
}

/// The repository's memories that bear on the decision, as lines for the
/// judge's picture (context, never evidence: see `common::stop_judge`).
/// Best effort: empty when the repository is unknown or has none.
async fn repo_memories(
    state: &WorkerState,
    team_id: &str,
    subject: &Subject,
    picture: &Picture,
) -> Vec<String> {
    let (repo, run_ref) = match subject {
        Subject::Pr { repo, number } => (repo.clone(), format!("PR #{number}")),
        Subject::Run { run_id } => {
            let repo = state
                .dynamo
                .get_item()
                .table_name(&state.config.runs_table_name)
                .key(
                    "team_id",
                    aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
                )
                .key(
                    "run_id",
                    aws_sdk_dynamodb::types::AttributeValue::S(run_id.clone()),
                )
                .projection_expression("#r")
                .expression_attribute_names("#r", "repo")
                .send()
                .await
                .ok()
                .and_then(|o| o.item()?.get("repo")?.as_s().ok().cloned())
                .unwrap_or_default();
            (repo, run_id.clone())
        }
    };
    let Some((owner, name)) = repo.split_once('/') else {
        return Vec::new();
    };
    let query = format!(
        "{}\n{}\n{}",
        picture.ticket_title,
        picture.reason,
        common::truncate_str(&picture.diff_summary, 1500)
    );
    crate::memory::recall::for_agent(
        state,
        crate::memory::recall::Agent::StopJudge,
        team_id,
        owner,
        name,
        &query,
        &run_ref,
        &picture.ticket_title,
    )
    .await
    .lines
}

/// Days of the outcome log the judge remembers.
const OVERTURNED_DAYS: i64 = 60;
/// Pages of the light decision query read for it.
const OVERTURNED_PAGES: usize = 3;

/// The team's recent decisions of this kind that people's acts showed
/// wrong, for the judge's picture: the outcome log closes the loop instead
/// of only feeding analytics. Best effort: empty on any failure.
async fn overturned(state: &WorkerState, team_id: &str, picture: &Picture) -> Vec<String> {
    let since = (chrono::Utc::now() - chrono::Duration::days(OVERTURNED_DAYS)).to_rfc3339();
    let records = decision_log::since_light(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        &since,
        OVERTURNED_PAGES,
    )
    .await;
    let lines = decision_log::overturned_lines(&records, picture.kind().as_str(), &picture.step);
    if !lines.is_empty() {
        info!(team_id, step = %picture.step, overturned = lines.len(), "Stop judge given the decisions people overturned");
    }
    lines
}

/// Records a judged decision (best effort).
async fn keep(
    state: &WorkerState,
    team_id: &str,
    subject: &Subject,
    head_sha: &str,
    picture: &Picture,
    judged: &Judged,
) {
    let rec = Record::new(subject, judged, picture, head_sha);
    if let judge::Source::Fallback(why) = judged.source() {
        warn!(team_id, step = judged.step(), why = %why, "Stop judge fell back");
    }
    info!(
        team_id,
        kind = judged.kind().as_str(),
        step = judged.step(),
        verdict = judged.verdict().as_str(),
        "stop_judge_decision"
    );
    decision_log::record(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        &rec,
    )
    .await;
}

/// A decision that is settled before judgment because going on is
/// impossible (the repository isn't connected, the run's ceiling is spent):
/// still recorded, no model call.
pub async fn cannot_proceed(
    state: &WorkerState,
    team_id: &str,
    subject: &Subject,
    kind: Kind,
    step: &str,
    why: &str,
) -> Judged {
    struct NoModel;
    impl JudgeModel for NoModel {
        async fn answer(&self, _: &str, _: &str, _: &Value) -> Result<Value, String> {
            Err("not called".into())
        }
    }
    let mut picture = Picture::new(kind, step, why);
    picture.cannot_proceed = why.to_string();
    let picture = picture.with_ids();
    let judged = judge::judge(&NoModel, picture.clone()).await;
    keep(state, team_id, subject, "", &picture, &judged).await;
    judged
}

/// [`decide`] for a step that holds no provider: the team's own, loaded
/// here. A team with no model configured gets the fallback (proceed for
/// reversible work), never a question.
pub async fn decide_for_team(
    state: &WorkerState,
    usage: &mut TokenUsage,
    team_id: &str,
    subject: &Subject,
    head_sha: &str,
    picture: Picture,
) -> Judged {
    match ModelProvider::load_for_team(&state.dynamo, &state.config.settings_table_name, team_id)
        .await
    {
        Ok(provider) => decide(state, &provider, usage, team_id, subject, head_sha, picture).await,
        Err(e) => {
            struct NoModel(String);
            impl JudgeModel for NoModel {
                async fn answer(&self, _: &str, _: &str, _: &Value) -> Result<Value, String> {
                    Err(self.0.clone())
                }
            }
            let picture = picture.with_ids();
            let judged = judge::judge(&NoModel(e.to_string()), picture.clone()).await;
            keep(state, team_id, subject, head_sha, &picture, &judged).await;
            judged
        }
    }
}

/// A picture for a question a step would ask: the ticket, the questions as
/// the proposal (what was tried, the options), and what each cites as
/// evidence (unchecked: a question's citations are why it asks, not proof).
pub fn ask_picture(
    step: &str,
    reason: &str,
    msg: &TicketMessage,
    questions: &[common::run_questions::Question],
) -> Picture {
    let mut p = ticket_picture(Kind::Ask, step, reason, msg);
    p.question = Some(common::stop_judge::ProposedQuestion {
        question: questions
            .iter()
            .map(|q| q.question.trim().to_string())
            .collect::<Vec<_>>()
            .join(" / "),
        tried: questions.iter().flat_map(|q| q.checked.clone()).collect(),
        options: questions
            .iter()
            .flat_map(|q| q.suggestions.clone())
            .collect(),
    });
    for q in questions {
        for e in q.evidence.iter().chain(q.written_at.iter()) {
            p.evidence.push(Evidence {
                source: "the question's citation".into(),
                finding: q.question.clone(),
                location: e.clone(),
                checked: false,
                ..Default::default()
            });
        }
        if !q.changes.trim().is_empty() {
            p.evidence.push(Evidence {
                source: "how the fix differs by answer".into(),
                finding: q.changes.clone(),
                ..Default::default()
            });
        }
    }
    p.related = msg
        .answers
        .iter()
        .map(|a| Related {
            ticket: "earlier answer".into(),
            summary: format!("{} -> {}", a.question, a.answer),
            ..Default::default()
        })
        .collect();
    p
}

/// The questions to post for a judged ask: the judge's own (with options,
/// a default and a deadline), else what the step proposed.
pub fn questions_for(
    judged: &Judged,
    proposed: &[common::run_questions::Question],
) -> Vec<common::run_questions::Question> {
    match judged.run_question() {
        Some(q) => vec![q],
        None => proposed.to_vec(),
    }
}

/// The text a judged stop posts: the judge's question (what was tried, the
/// options, the default and when it is taken), or the named blocker.
pub fn stop_text(judged: &Judged, fallback: &str) -> String {
    if let Some(md) = judged.question_markdown() {
        return md;
    }
    match judged.hold_words() {
        Some(h) => format!("**Blocked:** {h}"),
        None => fallback.to_string(),
    }
}

/// Answers that record a judged assumption for each proposed question, so a
/// re-plan reads them like a person's answers.
pub fn assumed_answers(
    judged: &Judged,
    questions: &[common::run_questions::Question],
) -> Vec<common::run_questions::Answer> {
    let Some(a) = judged.assumption() else {
        return Vec::new();
    };
    let now = chrono::Utc::now().to_rfc3339();
    let qs: Vec<common::run_questions::Question> = if questions.is_empty() {
        vec![common::run_questions::Question {
            id: "q1".into(),
            question: "What should the run assume?".into(),
            ..Default::default()
        }]
    } else {
        questions.to_vec()
    };
    qs.iter()
        .map(|q| common::run_questions::Answer {
            question_id: q.id.clone(),
            question: q.question.clone(),
            answer: format!("Not asked; assumed: {}", a.trim()),
            author: "CoderHelm (stop judge)".into(),
            answered_at: now.clone(),
            decision: String::new(),
        })
        .collect()
}

/// Keeps a judged question's default and deadline on the run, so the
/// stuck-run sweep can take the default once the deadline passes on
/// reversible work (`lambda/stuck-run-cleanup`).
pub async fn keep_ask_deadline(state: &WorkerState, team_id: &str, run_id: &str, judged: &Judged) {
    let Some(q) = judged.question() else {
        return;
    };
    let due = chrono::Utc::now() + chrono::Duration::hours(i64::from(q.deadline_hours));
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression("SET ask_default = :d, ask_deadline = :t, ask_question = :q")
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_values(":d", super::attr_s(&q.default))
        .expression_attribute_values(":t", super::attr_s(&due.to_rfc3339()))
        .expression_attribute_values(":q", super::attr_s(&q.question))
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not keep the question's default and deadline");
    }
}

/// The assumptions the judge proceeded on in this run, as Open items for
/// the pull request description: each one a person can correct.
pub async fn assumptions(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
) -> Vec<super::proof::Finding> {
    let records = decision_log::for_subject(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        &run_subject(run_id),
    )
    .await;
    assumption_items(&records)
}

/// Open items from decision records: one per proceed, newest wording kept.
pub fn assumption_items(records: &[Record]) -> Vec<super::proof::Finding> {
    let mut out: Vec<super::proof::Finding> = Vec::new();
    for r in records.iter().filter(|r| r.verdict == "proceed") {
        if r.detail.trim().is_empty() || out.iter().any(|f| f.detail.contains(r.detail.trim())) {
            continue;
        }
        out.push(super::proof::Finding {
            point: super::proof::JUDGE_ASSUMPTION.to_string(),
            detail: format!("{}: {}", r.step.replace('_', " "), r.detail.trim()),
            settle: "Correct it in a comment on this pull request if it's wrong; otherwise                      delete this line."
                .to_string(),
            ..Default::default()
        });
    }
    out
}

/// What a waiting run shows for a judged stop: the questions to answer, or
/// the named blocker as the action.
pub fn wait_for(
    judged: &Judged,
    questions: &[common::run_questions::Question],
    where_asked: Option<&str>,
    url: &str,
) -> common::run_wait::WaitingOn {
    let fallback = common::run_wait::WaitingOn::fixed(
        "Unblock the run",
        &judged
            .hold_words()
            .unwrap_or_else(|| "the stop judge named what is needed".into()),
        url,
    );
    if judged.hold().is_some() {
        return fallback;
    }
    common::run_wait::WaitingOn::answer_or(questions, where_asked, fallback)
}

/// A picture for handing a run to a person: the ticket, the reason, and
/// what the checks left open as evidence (a check that didn't pass is
/// checked evidence; the rest is what remains unproven).
pub fn handoff_picture(
    msg: &TicketMessage,
    step: &str,
    reason: &str,
    open: &[super::proof::Finding],
) -> Picture {
    let mut p = ticket_picture(Kind::Handoff, step, reason, msg);
    p.evidence = open
        .iter()
        .map(|f| Evidence {
            source: format!("check ({})", f.point),
            finding: f.detail.clone(),
            location: f.check.clone(),
            checked: f.point == "check_blocked" || f.point == "rule_failing",
            ..Default::default()
        })
        .collect();
    p
}

/// The ticket of a waiting run, started again with `answers` (the default
/// once a question's deadline passed): the same message a person's Retry
/// builds from the run record.
pub fn restart_message(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    answers: Vec<common::run_questions::Answer>,
) -> Option<TicketMessage> {
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let n = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let (owner, name) = s("repo")
        .split_once('/')
        .map(|(a, b)| (a.to_string(), b.to_string()))?;
    let source = match s("ticket_source").as_str() {
        "jira" => crate::models::TicketSource::Jira,
        "alert" => crate::models::TicketSource::Alert,
        _ => crate::models::TicketSource::Github,
    };
    let carries_body = !matches!(source, crate::models::TicketSource::Github);
    let (repo_owner, repo_name) = if matches!(source, crate::models::TicketSource::Jira) {
        (String::new(), String::new())
    } else {
        (owner, name)
    };
    Some(TicketMessage {
        team_id: s("team_id"),
        installation_id: n("installation_id"),
        source,
        ticket_id: s("ticket_id"),
        title: s("title"),
        body: if carries_body {
            s("ticket_body")
        } else {
            String::new()
        },
        repo_owner,
        repo_name,
        issue_number: n("issue_number"),
        sender: "coderhelm".into(),
        requester: Some(common::requester::Requester::coderhelm("deadline")),
        answers,
        new_run_id: Some(ulid::Ulid::new().to_string()),
        ..Default::default()
    })
}

/// A judged question's deadline passed with no answer (the stuck-run sweep
/// claimed it): reversible work goes ahead with the recommended default.
/// The waiting run is marked started again, and its ticket is queued with
/// the default as the answer.
pub async fn take_default(state: &WorkerState, team_id: &str, run_id: &str) {
    let Ok(out) = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(team_id))
        .key("run_id", super::attr_s(run_id))
        .send()
        .await
    else {
        return;
    };
    let Some(item) = out.item else {
        return;
    };
    let text = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    if text("status") != common::run_status::NEEDS_INPUT || text("ask_default").trim().is_empty() {
        return;
    }
    let answer = common::run_questions::Answer {
        question_id: "q1".into(),
        question: text("ask_question"),
        answer: text("ask_default"),
        author: "CoderHelm (no answer by the deadline; the recommended option)".into(),
        answered_at: chrono::Utc::now().to_rfc3339(),
        decision: String::new(),
    };
    let Some(mut ticket) = restart_message(&item, vec![answer]) else {
        warn!(
            run_id,
            "Deadline passed but the run's ticket couldn't be rebuilt"
        );
        return;
    };
    ticket.team_id = team_id.to_string();
    let new_run = ticket.new_run_id.clone().unwrap_or_default();
    // Only while it still waits: an answer that arrived first wins.
    let claimed = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression("SET #s = :r, status_run_id = :sri, retried_as = :n, updated_at = :t")
        .condition_expression("#s = :ni")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":r", super::attr_s(common::run_status::RETRIED))
        .expression_attribute_values(
            ":sri",
            super::attr_s(&format!("{}#{run_id}", common::run_status::RETRIED)),
        )
        .expression_attribute_values(":n", super::attr_s(&new_run))
        .expression_attribute_values(":t", super::attr_s(&chrono::Utc::now().to_rfc3339()))
        // A condition, not a write: only while it still waits.
        .expression_attribute_values(
            ":ni",
            aws_sdk_dynamodb::types::AttributeValue::S(common::run_status::NEEDS_INPUT.into()),
        )
        .send()
        .await
        .is_ok();
    if !claimed {
        return;
    }
    let Ok(body) = serde_json::to_string(&crate::models::WorkerMessage::Ticket(ticket)) else {
        return;
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
    {
        Ok(_) => {
            super::add_progress_note(
                state,
                team_id,
                run_id,
                &format!(
                    "No answer by the deadline: going ahead with the recommended option ({}) in run {new_run}",
                    text("ask_default")
                ),
            )
            .await
        }
        Err(e) => warn!(run_id, error = %e, "Could not queue the ticket with the default answer"),
    }
}

/// The run a ticket message belongs to, as a decision subject.
pub fn run_subject(run_id: &str) -> Subject {
    Subject::Run {
        run_id: run_id.to_string(),
    }
}

/// A pull request, as a decision subject.
pub fn pr_subject(owner: &str, name: &str, number: u64) -> Subject {
    Subject::Pr {
        repo: format!("{owner}/{name}"),
        number,
    }
}

/// The ticket's part of a picture: title, body, and the acceptance criteria
/// the body lists (lines under an "Acceptance" heading, or checkbox lines).
pub fn ticket_picture(kind: Kind, step: &str, reason: &str, msg: &TicketMessage) -> Picture {
    let mut p = Picture::new(kind, step, reason);
    p.ticket_title = msg.title.clone();
    p.ticket_body = msg.body.clone();
    p.acceptance = acceptance(&msg.body);
    p
}

/// Acceptance criteria a ticket body lists.
pub fn acceptance(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut under = false;
    for line in body.lines() {
        let t = line.trim();
        let lower = t.to_ascii_lowercase();
        if t.starts_with('#') || (t.ends_with(':') && t.len() < 60) {
            under = lower.contains("acceptance") || lower.contains("done when");
            continue;
        }
        let item = t
            .strip_prefix("- [ ]")
            .or_else(|| t.strip_prefix("- [x]"))
            .or_else(|| t.strip_prefix("* [ ]"))
            .map(str::trim)
            .or_else(|| {
                under
                    .then(|| t.strip_prefix("- ").or_else(|| t.strip_prefix("* ")))
                    .flatten()
            });
        if let Some(i) = item.filter(|i| !i.is_empty()) {
            out.push(i.to_string());
        }
    }
    out
}

/// Leave to start a fix cycle. The first cycle of a check needs none; any
/// later one only from a judged hold (the judge named the blocker worth one
/// more cycle).
#[derive(Debug)]
pub struct CycleLicence {
    _sealed: (),
}

impl CycleLicence {
    /// The first cycle of a check.
    pub fn first(rounds_done: u32) -> Option<Self> {
        (rounds_done == 0).then_some(Self { _sealed: () })
    }

    /// A later cycle, when the judge held on a named blocker.
    pub fn judged(j: &Judged) -> Option<Self> {
        (j.kind() == Kind::ExtraCycle && j.hold().is_some()).then_some(Self { _sealed: () })
    }

    /// For a test that drives a ledger directly.
    #[cfg(test)]
    pub fn for_test() -> Self {
        Self { _sealed: () }
    }
}

/// Plays back recorded answers, in order (the replay suite and tests).
/// [`next_cycle`] for a check that holds no provider: the licence for its
/// next fix cycle, or `None` to deliver what exists with what is still open
/// listed (the judge's assumption joins the Open items).
#[allow(clippy::too_many_arguments)]
pub async fn cycle_licence(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    step: &str,
    rounds_done: u32,
    max_rounds: u32,
    open: &[Evidence],
) -> Option<CycleLicence> {
    if let Some(l) = CycleLicence::first(rounds_done) {
        return Some(l);
    }
    let mut p = ticket_picture(
        Kind::ExtraCycle,
        step,
        &format!(
            "the {} check still reports {} open item(s) after {rounds_done} fix cycle(s)",
            step.replace('_', " "),
            open.len()
        ),
        msg,
    );
    p.evidence = open.to_vec();
    p.budget = Budget {
        cycles: rounds_done,
        cycle_cap: max_rounds,
        ..Default::default()
    };
    let mut usage = TokenUsage::default();
    let j = decide_for_team(state, &mut usage, &msg.team_id, &run_subject(run_id), "", p).await;
    CycleLicence::judged(&j)
}

/// A check's open items as evidence: what the check ran found, so checked.
pub fn evidence_lines(source: &str, items: &[String]) -> Vec<Evidence> {
    items
        .iter()
        .map(|i| Evidence {
            source: source.to_string(),
            finding: i.clone(),
            checked: true,
            ..Default::default()
        })
        .collect()
}

#[cfg(test)]
pub struct Recorded(pub Mutex<Vec<Result<Value, String>>>);

#[cfg(test)]
impl Recorded {
    pub fn new(answers: Vec<Value>) -> Self {
        Self(Mutex::new(answers.into_iter().rev().map(Ok).collect()))
    }
}

#[cfg(test)]
impl JudgeModel for Recorded {
    async fn answer(&self, _: &str, _: &str, _: &Value) -> Result<Value, String> {
        self.0
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(|| Err("no recorded answer left".into()))
    }
}

/// A judged decision from recorded answers, for tests.
#[cfg(test)]
pub fn judged_from(picture: Picture, answers: Vec<Value>) -> Judged {
    let model = Recorded::new(answers);
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(judge::judge(&model, picture))
}

/// A hold on evidence E1 for `kind`, for tests.
#[cfg(test)]
pub fn test_hold(kind: Kind) -> Judged {
    let mut p = Picture::new(kind, "test", "a checked problem");
    p.evidence = vec![Evidence {
        source: "ci".into(),
        finding: "tests fail".into(),
        location: "src/a.rs:1".into(),
        checked: true,
        ..Default::default()
    }];
    judged_from(
        p,
        vec![
            serde_json::json!({"verdict":"hold","assumption":"","question":"","person_only":"",
            "tried":[],"options":[],"default":"","deadline_hours":0,"blocker":"tests fail",
            "evidence":["E1"],"category":"correctness","location":"src/a.rs:1"}),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }

    /// One function of a source file: its signature line and its body.
    struct Func {
        sig: String,
        body: String,
        line: usize,
    }

    fn starts_fn(line: &str) -> bool {
        let t = line
            .trim_start()
            .trim_start_matches("pub(crate) ")
            .trim_start_matches("pub(super) ")
            .trim_start_matches("pub ")
            .trim_start_matches("async ");
        t.starts_with("fn ")
    }

    /// The non-test functions of a file, each with its signature (up to the
    /// opening brace) and body.
    fn functions(src: &str) -> Vec<Func> {
        // Test modules (`#[cfg(test)]` then `mod x {`, to its closing brace
        // at column 0) are blanked; a declaration of a test-only file
        // (`mod x;`) is not one.
        let all: Vec<&str> = src.lines().collect();
        let mut lines: Vec<&str> = Vec::with_capacity(all.len());
        let mut i = 0;
        while i < all.len() {
            let opens = all[i].trim() == "#[cfg(test)]"
                && all.get(i + 1).is_some_and(|n| {
                    n.trim_start().starts_with("mod ") && n.trim_end().ends_with('{')
                });
            if opens {
                while i < all.len() && all[i] != "}" {
                    lines.push("");
                    i += 1;
                }
            }
            if i < all.len() {
                lines.push(if opens { "" } else { all[i] });
            }
            i += 1;
        }
        let starts: Vec<usize> = (0..lines.len()).filter(|&i| starts_fn(lines[i])).collect();
        let mut out = Vec::new();
        for (k, &s) in starts.iter().enumerate() {
            let end = starts.get(k + 1).copied().unwrap_or(lines.len());
            let sig_end = (s..end).find(|&j| lines[j].contains('{')).unwrap_or(s);
            out.push(Func {
                sig: lines[s..=sig_end].join("\n"),
                body: lines[s..end].join("\n"),
                line: s + 1,
            });
        }
        out
    }

    /// What doing a costly thing looks like in source. Since every
    /// `needs_input` write goes through `common::run_wait::NeedsInput`, the
    /// call is the write.
    const NEEDS_INPUT_WRITES: &[&str] = &[".needs_input(", "attr_s(\"needs_input\")"];

    /// A line that is only the status, as an argument or a value
    /// (`complete_run_with_status(.., NEEDS_INPUT, ..)`, `else { "needs_input" }`).
    fn bare_status(line: &str) -> bool {
        matches!(
            line.trim().trim_end_matches(','),
            "\"needs_input\"" | "run_status::NEEDS_INPUT" | "common::run_status::NEEDS_INPUT"
        )
    }
    const ASKS: &[&str] = &[
        "run_questions::FIELD_QUESTIONS",
        "record_questions(",
        "ask_on_ticket(",
    ];

    /// The function is handed a judged decision.
    fn takes_judged(sig: &str) -> bool {
        sig.contains("Judged")
    }

    /// Problems in one file.
    fn check(rel: &str, src: &str) -> Vec<String> {
        let mut problems = Vec::new();
        for f in functions(src) {
            let code: String = f
                .body
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n");
            let name = f.sig.lines().next().unwrap_or("").trim().to_string();
            let defines = |pat: &str| {
                let fn_name = pat.trim_end_matches('(');
                name.contains(&format!("fn {fn_name}"))
            };
            let bare = code.lines().any(bare_status);
            if bare && !takes_judged(&f.sig) {
                problems.push(format!(
                    "{rel}:{}: `{name}` sets needs_input without a judged decision",
                    f.line
                ));
            }
            for pat in NEEDS_INPUT_WRITES {
                if code.contains(pat) && !takes_judged(&f.sig) {
                    problems.push(format!(
                        "{rel}:{}: `{name}` writes needs_input without a judged decision",
                        f.line
                    ));
                }
            }
            // Asking a person goes through the ask helpers, which take a
            // judged decision (so every call has one, or doesn't compile);
            // writing the questions on the run happens only with one.
            for pat in ASKS {
                let defined = defines(pat);
                let writes = *pat == "run_questions::FIELD_QUESTIONS" && code.contains(pat);
                if (defined || writes) && !takes_judged(&f.sig) {
                    problems.push(format!(
                        "{rel}:{}: `{name}` asks a person (`{pat}`) without a judged decision",
                        f.line
                    ));
                }
            }
            if code.contains(".start_fix(")
                && !code.contains("CycleLicence")
                && !code.contains("stop_judge::cycle_licence(")
                && !takes_judged(&f.sig)
            {
                problems.push(format!(
                    "{rel}:{}: `{name}` starts a fix cycle without a CycleLicence",
                    f.line
                ));
            }
        }
        problems
    }

    #[test]
    fn every_costly_decision_goes_through_the_judge() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        walk(&root, &mut files);
        assert!(files.len() > 50, "found the sources");
        let mut problems = Vec::new();
        for f in files {
            let rel = f
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if rel.starts_with("passes/incidents/")
                || rel.starts_with("passes/replay/")
                || rel == "passes/stop_judge.rs"
            {
                continue;
            }
            problems.extend(check(&rel, &std::fs::read_to_string(&f).unwrap()));
        }
        assert!(
            problems.is_empty(),
            "costly decisions made without the stop judge:\n{}",
            problems.join("\n")
        );
    }

    #[test]
    fn the_check_catches_each_way_around_the_judge() {
        let bad = "async fn stop(state: &S) {\n    update.needs_input(run_id, &wait);\n}\n";
        assert_eq!(check("a.rs", bad).len(), 1);
        let good = "async fn stop(state: &S, judged: &Judged) {\n    update.needs_input(run_id, &wait);\n}\n";
        assert!(check("a.rs", good).is_empty());
        let ask = "async fn ask_on_ticket(state: &S, q: &[Q]) {\n    post(q).await;\n}\n";
        assert_eq!(check("a.rs", ask).len(), 1);
        let write = "async fn keep(state: &S) {\n    let n = run_questions::FIELD_QUESTIONS;\n}\n";
        assert_eq!(check("a.rs", write).len(), 1);
        let def = "async fn record_questions(state: &S, judged: &Judged) {\n    let _ = run_questions::FIELD_QUESTIONS;\n}\n";
        assert!(check("a.rs", def).is_empty());
        let cycle = "fn f(l: &L) {\n    l.update(|l| l.proof.start_fix());\n}\n";
        assert_eq!(check("a.rs", cycle).len(), 1);
        let licensed =
            "fn f(l: &L, lic: CycleLicence) {\n    l.update(|l| l.proof.start_fix(lic));\n}\n";
        assert!(check("a.rs", licensed).is_empty());
        // Test modules are not scanned; a test-only file's declaration is
        // not a test module.
        let test = "fn real() {}\n#[cfg(test)]\nmod t {\n    fn x() { u.needs_input(r, &w); }\n}\n";
        assert!(check("a.rs", test).is_empty());
        let decl = "#[cfg(test)]\nmod cases;\nfn f() {\n    u.needs_input(r, &w);\n}\n";
        assert_eq!(check("a.rs", decl).len(), 1);
    }

    #[test]
    fn the_review_post_refuses_changes_requested_without_a_judged_hold() {
        let src = include_str!("review_verdict.rs");
        let post = src
            .split("pub async fn post(")
            .nth(1)
            .expect("review_verdict::post");
        let sig = &post[..post.find('{').unwrap()];
        assert!(sig.contains("judged: Option<&Judged>"), "{sig}");
        assert!(post.contains("may_post(event, verdict, judged)"));
    }

    /// Every judgment step, by file and function: each must run on the
    /// strongest configured model (`judgment_model_id`) and never at an
    /// effort below high. A cheaper model or a lower effort here is a
    /// downgrade the replay suite must clear first (`passes::replay`).
    const JUDGMENT_STEPS: &[(&str, &str, &str)] = &[
        ("passes/plan.rs", "pub async fn run(", "the planner"),
        (
            "passes/plan.rs",
            "async fn reask_decision(",
            "the planner's fix-location decision",
        ),
        (
            "passes/plan_judge.rs",
            "pub async fn judge(",
            "the plan's own check",
        ),
        (
            "passes/triage.rs",
            "pub async fn run(",
            "triage (route and size)",
        ),
        (
            "passes/triage.rs",
            "pub async fn select_repo(",
            "triage (which repository)",
        ),
        (
            "passes/mod.rs",
            "async fn validate_plan(",
            "the plan's repository check",
        ),
        (
            "passes/stop_gate.rs",
            "pub async fn screen_questions(",
            "question screening",
        ),
        (
            "passes/stop_gate.rs",
            "pub async fn existing_work(",
            "covered-by-PR check",
        ),
        (
            "passes/resolved.rs",
            "pub async fn judge(",
            "settled-question check",
        ),
        (
            "passes/review_severity.rs",
            "pub async fn second_pass(",
            "severity check",
        ),
        (
            "passes/review_pr.rs",
            "async fn classify_ask(",
            "ask classification",
        ),
        (
            "passes/review_pr.rs",
            "async fn reply_conclusion(",
            "a reply's verdict",
        ),
        (
            "passes/jira_comment.rs",
            "async fn classify(",
            "ticket comment intent",
        ),
        (
            "passes/open_pr_choice.rs",
            "async fn classify(",
            "a person's choice",
        ),
        (
            "passes/branch_update.rs",
            "pub(crate) async fn asked(",
            "branch update asks",
        ),
        (
            "passes/feature/reuse.rs",
            "pub async fn judge(",
            "duplicate-helper check",
        ),
        (
            "passes/security.rs",
            "pub async fn run(",
            "the security review",
        ),
    ];

    fn fn_body<'a>(src: &'a str, sig: &str) -> &'a str {
        let i = src.find(sig).unwrap_or_else(|| panic!("{sig} not found"));
        let rest = &src[i..];
        &rest[..rest.find("\n}\n").map(|j| j + 3).unwrap_or(rest.len())]
    }

    #[test]
    fn judgment_steps_run_on_the_strongest_model_at_high_effort() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        for (file, sig, what) in JUDGMENT_STEPS {
            let src = std::fs::read_to_string(root.join(file)).unwrap();
            let body = fn_body(&src, sig);
            assert!(
                body.contains("judgment_model_id()"),
                "{what} ({file}) must run on judgment_model_id()"
            );
            for low in [
                "primary_model_id()",
                "light_model_id",
                "Some(\"low\")",
                "Some(\"medium\")",
                "\n        \"low\",",
                "\n        \"medium\",",
            ] {
                assert!(
                    !body.contains(low),
                    "{what} ({file}) uses {low}: a judgment step runs on the strongest model at high effort"
                );
            }
        }
        // Review writing and the reviewer's critic too.
        let provider = std::fs::read_to_string(root.join("agent/provider.rs")).unwrap();
        let review = fn_body(&provider, "pub fn review_model_id(");
        assert!(review.contains("self.judgment_model_id()"));
        let skills = std::fs::read_to_string(root.join("passes/skills.rs")).unwrap();
        assert!(fn_body(&skills, "pub fn review_effort(").contains("JUDGMENT_EFFORT"));
        assert_eq!(JUDGMENT_EFFORT, "high");
    }

    #[test]
    fn a_cycle_after_the_first_needs_a_judged_hold() {
        assert!(CycleLicence::first(0).is_some());
        assert!(CycleLicence::first(1).is_none());
    }

    #[test]
    fn acceptance_criteria_are_read_from_the_body() {
        let body = "Make exports work.\n\n## Acceptance criteria\n- CSV download\n- Totals row\n\nNotes:\n- not this\n- [ ] boxes count anywhere";
        assert_eq!(
            acceptance(body),
            vec!["CSV download", "Totals row", "boxes count anywhere"]
        );
    }
}
