//! The reviewer learns from people's pushback, with no one rating anything.
//!
//! The gateway queues a judgment when a person replies under one of
//! CoderHelm's review comments, comments on a PR it reviewed, dismisses its
//! review, or closes the PR while its "changes requested" stands (debounced
//! per PR, see `common::review_lessons`). This pass reads the findings, the
//! diff hunks and code around them, the PR description and what people wrote,
//! and a model decides whether the reviewer was wrong. When it was, one
//! general lesson is kept for the repository (or the whole team): a second
//! model call keeps the list free of repeats and lets a newer lesson replace
//! one it contradicts. Every later review of the repository, and its critic,
//! is given the lessons (`common::review_lessons::recall_block`).
//!
//! Mirrors `lessons.rs` (the planner's lessons from closed PRs), but the
//! lessons live on the settings table: they hold for a whole team as well as
//! one repository, the dashboard lists and deletes them, and they never roll
//! out of a window of recent reviews.

use crate::agent::provider::{self, ModelProvider};
use crate::clients::github::GitHubClient;
use crate::models::{ReviewLessonMessage, ReviewMessage, TokenUsage};
use crate::passes::attr_s;
use crate::WorkerState;
use common::review_lessons::{self as rl, Reconcile, ReviewLesson};
use serde::Deserialize;
use serde_json::Value;
use tracing::{info, warn};

/// Most findings handed to the judge.
const MAX_FINDINGS: usize = 8;
/// Most files read for the code around a finding.
const MAX_CODE_READS: usize = 4;
/// Lines of code shown on each side of a finding's line.
const CODE_CONTEXT_LINES: usize = 25;
/// Most PR conversation comments handed to the judge.
const MAX_CONVERSATION: usize = 12;

/// A model the judgment asks. The worker's is the team's model; tests use a
/// stub.
#[async_trait::async_trait]
pub trait Model: Send {
    async fn ask(&mut self, system: &str, prompt: &str) -> Option<String>;
}

/// The team's model, counting what it uses.
pub struct TeamModel<'a> {
    pub state: &'a WorkerState,
    pub provider: &'a ModelProvider,
    pub usage: &'a mut TokenUsage,
}

#[async_trait::async_trait]
impl Model for TeamModel<'_> {
    async fn ask(&mut self, system: &str, prompt: &str) -> Option<String> {
        match provider::converse_simple(
            self.state,
            self.provider,
            self.provider.primary_model_id(),
            system,
            prompt,
            self.usage,
        )
        .await
        {
            Ok(t) => Some(t),
            Err(e) => {
                warn!(error = %e, "Review lesson model call failed");
                None
            }
        }
    }
}

/// One of CoderHelm's review comments and what people wrote under it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DisputedFinding {
    pub path: String,
    pub line: u64,
    pub diff_hunk: String,
    /// CoderHelm's comment.
    pub body: String,
    /// People's replies under it, as (author, text), oldest first.
    pub replies: Vec<(String, String)>,
    /// The code around the line at the PR's head (filled in later).
    pub code: String,
}

/// What the judge is shown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Discussion {
    pub pr_title: String,
    pub pr_body: String,
    pub head_sha: String,
    /// CoderHelm's review summaries, as (state, body), newest last.
    pub reviews: Vec<(String, String)>,
    pub findings: Vec<DisputedFinding>,
    /// People's PR conversation comments, as (author, text).
    pub conversation: Vec<(String, String)>,
    pub closed_unmerged: bool,
    /// CoderHelm's latest formal state is "changes requested".
    pub changes_requested: bool,
    /// A person dismissed CoderHelm's review.
    pub dismissed: bool,
}

impl Discussion {
    /// Is there anything people did against the review? Without it there is
    /// nothing to judge.
    pub fn has_pushback(&self) -> bool {
        self.findings.iter().any(|f| !f.replies.is_empty())
            || !self.conversation.is_empty()
            || self.dismissed
            || (self.closed_unmerged && self.changes_requested)
    }
}

/// A person, not a bot account and not CoderHelm.
fn is_person(user: &Value) -> bool {
    user["type"].as_str() != Some("Bot")
        && !user["login"]
            .as_str()
            .unwrap_or("coderhelm")
            .contains("coderhelm")
}

fn is_coderhelm(user: &Value) -> bool {
    user["login"].as_str().unwrap_or("").contains("coderhelm")
}

/// Pure: the discussion from GitHub's PR, its reviews, its review comments
/// and its conversation comments.
pub fn discussion_from(
    pr: &Value,
    reviews: &[Value],
    review_comments: &[Value],
    issue_comments: &[Value],
) -> Discussion {
    let mine: Vec<&Value> = reviews
        .iter()
        .filter(|r| is_coderhelm(&r["user"]))
        .collect();
    // The latest formal state: approvals and change requests (and their
    // dismissal) count; a plain comment review leaves the state as it was.
    let latest_formal = mine
        .iter()
        .rev()
        .map(|r| r["state"].as_str().unwrap_or(""))
        .find(|s| matches!(*s, "APPROVED" | "CHANGES_REQUESTED" | "DISMISSED"))
        .unwrap_or("");
    let review_bodies: Vec<(String, String)> = mine
        .iter()
        .filter_map(|r| {
            let body = r["body"].as_str().unwrap_or("").trim();
            (!body.is_empty()).then(|| {
                (
                    r["state"].as_str().unwrap_or("").to_string(),
                    common::head_tail_str(body, 3000),
                )
            })
        })
        .collect();
    let review_bodies = review_bodies[review_bodies.len().saturating_sub(2)..].to_vec();

    // CoderHelm's top-level comments, with the people's replies under them.
    let mut findings: Vec<(u64, DisputedFinding)> = review_comments
        .iter()
        .filter(|c| c["in_reply_to_id"].is_null() && is_coderhelm(&c["user"]))
        .map(|c| {
            (
                c["id"].as_u64().unwrap_or(0),
                DisputedFinding {
                    path: c["path"].as_str().unwrap_or("").to_string(),
                    line: c["line"]
                        .as_u64()
                        .or_else(|| c["original_line"].as_u64())
                        .unwrap_or(0),
                    diff_hunk: c["diff_hunk"].as_str().unwrap_or("").to_string(),
                    body: c["body"].as_str().unwrap_or("").to_string(),
                    ..Default::default()
                },
            )
        })
        .collect();
    for c in review_comments {
        let Some(parent) = c["in_reply_to_id"].as_u64() else {
            continue;
        };
        let body = c["body"].as_str().unwrap_or("").trim();
        if !is_person(&c["user"]) || body.is_empty() {
            continue;
        }
        if let Some((_, f)) = findings.iter_mut().find(|(id, _)| *id == parent) {
            f.replies.push((
                c["user"]["login"].as_str().unwrap_or("").to_string(),
                body.to_string(),
            ));
        }
    }
    // Disputed findings first, then the rest (newest first) up to the cap.
    let (mut disputed, mut rest): (Vec<_>, Vec<_>) = findings
        .into_iter()
        .partition(|(_, f)| !f.replies.is_empty());
    rest.reverse();
    disputed.append(&mut rest);
    let findings: Vec<DisputedFinding> = disputed
        .into_iter()
        .map(|(_, f)| f)
        .take(MAX_FINDINGS)
        .collect();

    // People's own reviews with words, and the PR conversation.
    let mut conversation: Vec<(String, String)> = reviews
        .iter()
        .filter(|r| is_person(&r["user"]))
        .filter_map(|r| {
            let body = r["body"].as_str().unwrap_or("").trim();
            (!body.is_empty()).then(|| {
                (
                    format!(
                        "{} (in a review)",
                        r["user"]["login"].as_str().unwrap_or("")
                    ),
                    body.to_string(),
                )
            })
        })
        .collect();
    conversation.extend(super::review_pr::human_comments(
        issue_comments,
        MAX_CONVERSATION,
    ));
    let conversation = conversation[conversation.len().saturating_sub(MAX_CONVERSATION)..].to_vec();
    Discussion {
        pr_title: pr["title"].as_str().unwrap_or("").to_string(),
        pr_body: pr["body"].as_str().unwrap_or("").to_string(),
        head_sha: pr["head"]["sha"].as_str().unwrap_or("").to_string(),
        reviews: review_bodies,
        findings,
        conversation,
        closed_unmerged: pr["state"].as_str() == Some("closed")
            && !pr["merged"].as_bool().unwrap_or(false),
        changes_requested: latest_formal == "CHANGES_REQUESTED",
        dismissed: latest_formal == "DISMISSED",
    }
}

const JUDGE_SYSTEM: &str = "You read a code review that an automated reviewer (CoderHelm) left on \
     a pull request, and what people did about it, and decide whether the reviewer was wrong. \
     Judge each finding people pushed back on: a reply under it, a PR comment about it, \
     dismissing the review, or closing the PR while the review requested changes. Verdicts: \
     \"reviewer_wrong\" — not a defect: the code does what the team intends, it uses a mechanism \
     the code base already has for exactly this (for example adding an entry to a list or set \
     the code keeps for that purpose), or the reviewer misread the code or the intent the PR \
     description or people state; \"overstated\" — a fair point, but not one to block or request \
     changes on (a note at most); \"finding_correct\" — the reviewer was right: the person is \
     mistaken, or agreed, thanked it, asked a question, or fixed the code; \"unclear\" — not \
     enough to tell. Judge from the code and the diff as well as what people said: a person \
     saying \"this is intended\" is evidence about intent, not proof the code is safe, so a real \
     security or data-loss defect stays finding_correct. A PR closed without any sign it was \
     because of the review (superseded, duplicate, no longer needed) teaches nothing: unclear. \
     For reviewer_wrong or overstated, write ONE lesson the reviewer will be given on every \
     later review: general and reusable, naming the kind of change and what to do instead. \
     Example: \"Adding an entry to an exclusion list the code already keeps is the team's \
     mechanism for hiding records; don't block it, at most suggest a comment saying why the \
     entry is there.\" No ids, values, names of people or customers, or ticket numbers from \
     this PR unless they define the convention. scope: \"repo\" when the lesson rests on this \
     repository's code or conventions, \"team\" when it is about how this team works in \
     general. category: the finding's category (correctness, security, performance, style, \
     tests, docs, maintainability, other). Output ONLY a fenced ```json block: \
     {\"judgments\":[{\"finding\":\"short title\",\"verdict\":\"reviewer_wrong|overstated|\
     finding_correct|unclear\",\"why\":\"one sentence\",\"lesson\":\"the lesson, or empty\",\
     \"scope\":\"repo|team\",\"category\":\"...\"}]}";

/// Pure: the judge's prompt.
pub fn judge_prompt(d: &Discussion) -> String {
    let mut out = format!(
        "## Pull request\n{}\n\n{}\n",
        d.pr_title.trim(),
        common::truncate_str(d.pr_body.trim(), 3000)
    );
    let mut state = Vec::new();
    if d.changes_requested {
        state.push("CoderHelm's review requests changes.");
    }
    if d.dismissed {
        state.push("A person dismissed CoderHelm's review.");
    }
    if d.closed_unmerged {
        state.push("The pull request was closed without being merged.");
    }
    if !state.is_empty() {
        out.push_str(&format!("\n{}\n", state.join(" ")));
    }
    if !d.reviews.is_empty() {
        out.push_str("\n## CoderHelm's review summary\n");
        for (st, body) in &d.reviews {
            out.push_str(&format!("[{st}]\n{body}\n\n"));
        }
    }
    if !d.findings.is_empty() {
        out.push_str("\n## CoderHelm's inline findings\n");
        for (i, f) in d.findings.iter().enumerate() {
            out.push_str(&format!(
                "\n### Finding {} — {}:{}\n{}\n",
                i + 1,
                f.path,
                f.line,
                common::truncate_str(f.body.trim(), 2000)
            ));
            if !f.diff_hunk.trim().is_empty() {
                out.push_str(&format!(
                    "\nDiff hunk:\n```diff\n{}\n```\n",
                    common::head_tail_str(f.diff_hunk.trim(), 2500)
                ));
            }
            if !f.code.trim().is_empty() {
                out.push_str(&format!(
                    "\nCode around it at the PR's head:\n```\n{}\n```\n",
                    common::head_tail_str(f.code.trim(), 4000)
                ));
            }
            if !f.replies.is_empty() {
                out.push_str("\nReplies:\n");
                for (a, t) in &f.replies {
                    out.push_str(&format!("- {a}: {}\n", common::truncate_str(t, 1500)));
                }
            }
        }
    }
    if !d.conversation.is_empty() {
        out.push_str("\n## What people wrote on the PR\n");
        for (a, t) in &d.conversation {
            out.push_str(&format!("- {a}: {}\n", common::truncate_str(t, 1500)));
        }
    }
    out
}

/// One judged finding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Judged {
    #[serde(default)]
    pub finding: String,
    #[serde(default)]
    pub verdict: String,
    #[serde(default)]
    pub why: String,
    #[serde(default)]
    pub lesson: String,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub category: String,
}

impl Judged {
    /// The lesson to keep: only when the reviewer was wrong or overstated it.
    pub fn lesson_to_store(&self) -> Option<&str> {
        let l = self.lesson.trim();
        (matches!(self.verdict.as_str(), "reviewer_wrong" | "overstated") && !l.is_empty())
            .then_some(l)
    }
}

#[derive(Deserialize)]
struct Judgments {
    #[serde(default)]
    judgments: Vec<Judged>,
}

fn json_block(text: &str) -> &str {
    match text.rfind("```json") {
        Some(s) => {
            let after = &text[s + 7..];
            after[..after.find("```").unwrap_or(after.len())].trim()
        }
        None => text.trim(),
    }
}

/// Reads the judge's reply; `None` when it isn't in the shape asked.
pub fn parse_judgments(text: &str) -> Option<Vec<Judged>> {
    serde_json::from_str::<Judgments>(json_block(text))
        .ok()
        .map(|j| j.judgments)
}

const RECONCILE_SYSTEM: &str = "You keep a short list of lessons a code reviewer learned from \
     a team's corrections. Given the kept lessons (each with an id) and a new one, say whether \
     the new lesson says the same thing as a kept one (duplicate_of: that id), and which kept \
     lessons it contradicts or makes obsolete (supersedes: their ids; the newer lesson wins). \
     Lessons about different kinds of change are neither. Output ONLY a fenced ```json block: \
     {\"duplicate_of\": \"id\" or null, \"supersedes\": [\"id\", ...]}";

/// Pure: the reconciler's prompt.
pub fn reconcile_prompt(existing: &[ReviewLesson], new: &ReviewLesson) -> String {
    let mut out = String::from("## Kept lessons\n");
    for l in existing {
        out.push_str(&format!(
            "- id {}: [{}] {}\n",
            l.id,
            l.category,
            common::truncate_str(l.lesson.trim(), 600)
        ));
    }
    out.push_str(&format!(
        "\n## New lesson\n[{}] {}\n",
        new.category,
        new.lesson.trim()
    ));
    out
}

pub fn parse_reconcile(text: &str) -> Option<Reconcile> {
    serde_json::from_str(json_block(text)).ok()
}

/// Asks the judge about the discussion. Empty when there is no pushback or
/// the model didn't answer.
pub async fn judge(model: &mut dyn Model, d: &Discussion) -> Vec<Judged> {
    if !d.has_pushback() {
        return Vec::new();
    }
    let Some(reply) = model.ask(JUDGE_SYSTEM, &judge_prompt(d)).await else {
        return Vec::new();
    };
    parse_judgments(&reply).unwrap_or_else(|| {
        warn!("Review lesson judge's reply didn't read");
        Vec::new()
    })
}

/// Pure: the lesson record for a judged finding of `owner/name`'s PR.
pub fn new_lesson(
    j: &Judged,
    owner: &str,
    name: &str,
    pr: u64,
    trigger: &str,
    id: &str,
    now: &str,
) -> Option<ReviewLesson> {
    let text = j.lesson_to_store()?;
    let scope = if j.scope.trim() == "team" {
        rl::TEAM_SCOPE.to_string()
    } else {
        format!("{owner}/{name}")
    };
    Some(ReviewLesson {
        sk: rl::lesson_sk(&scope, id),
        id: id.to_string(),
        scope,
        lesson: common::truncate_str(text, 1000).to_string(),
        category: common::truncate_str(j.category.trim(), 40).to_string(),
        why: common::truncate_str(j.why.trim(), 1000).to_string(),
        source_repo: format!("{owner}/{name}"),
        source_pr: pr,
        trigger: trigger.to_string(),
        created_at: now.to_string(),
        seen: 1,
        last_seen_at: now.to_string(),
    })
}

/// Decides how a new lesson lands among the kept ones (asking the model only
/// when there are kept ones to compare against).
pub async fn plan_for(
    model: &mut dyn Model,
    existing: &[ReviewLesson],
    new: ReviewLesson,
) -> rl::StorePlan {
    let decision = if existing.is_empty() {
        Reconcile::default()
    } else {
        match model
            .ask(RECONCILE_SYSTEM, &reconcile_prompt(existing, &new))
            .await
        {
            Some(t) => parse_reconcile(&t).unwrap_or_default(),
            // No answer: keep the new lesson rather than lose it.
            None => Reconcile::default(),
        }
    };
    rl::plan_store(existing, new, &decision, rl::MAX_LESSONS_PER_SCOPE)
}

async fn apply_plan(state: &WorkerState, team_id: &str, plan: &rl::StorePlan) {
    let table = &state.config.settings_table_name;
    if let Some(l) = &plan.put {
        if let Err(e) = state
            .dynamo
            .put_item()
            .table_name(table)
            .set_item(Some(l.to_item(team_id)))
            .send()
            .await
        {
            warn!(error = %e, "Could not store a review lesson");
        }
    }
    if let Some(sk) = &plan.seen_again {
        let _ = state
            .dynamo
            .update_item()
            .table_name(table)
            .key("pk", attr_s(team_id))
            .key("sk", attr_s(sk))
            .update_expression("ADD seen :one SET last_seen_at = :t")
            .condition_expression("attribute_exists(sk)")
            .expression_attribute_values(":one", crate::passes::attr_n(1))
            .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
            .send()
            .await;
    }
    for sk in &plan.delete {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(table)
            .key("pk", attr_s(team_id))
            .key("sk", attr_s(sk))
            .send()
            .await;
    }
}

/// Reads the PR's discussion from GitHub, with the code around each disputed
/// finding.
async fn load_discussion(
    github: &GitHubClient,
    owner: &str,
    name: &str,
    pr_number: u64,
) -> Option<Discussion> {
    let pr = github.get_pull_request(owner, name, pr_number).await.ok()?;
    let reviews = github
        .list_pr_reviews(owner, name, pr_number)
        .await
        .ok()
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    let review_comments = github
        .get_review_comments(owner, name, pr_number)
        .await
        .unwrap_or_default();
    let issue_comments = github
        .list_issue_comments(owner, name, pr_number)
        .await
        .unwrap_or_default();
    let mut d = discussion_from(&pr, &reviews, &review_comments, &issue_comments);
    let head = d.head_sha.clone();
    if head.is_empty() {
        return Some(d);
    }
    for f in d
        .findings
        .iter_mut()
        .filter(|f| !f.replies.is_empty() && !f.path.is_empty())
        .take(MAX_CODE_READS)
    {
        let line = f.line.max(1) as usize;
        if let Ok(code) = github
            .read_file_lines(
                owner,
                name,
                &f.path,
                &head,
                line.saturating_sub(CODE_CONTEXT_LINES).max(1),
                line + CODE_CONTEXT_LINES,
            )
            .await
        {
            f.code = code;
        }
    }
    Some(d)
}

/// Judges a PR's discussion and keeps what was learned. Returns the lessons
/// stored (or counted again).
#[allow(clippy::too_many_arguments)]
async fn learn(
    state: &WorkerState,
    model: &mut dyn Model,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
    trigger: &str,
) -> Vec<String> {
    let Some(d) = load_discussion(github, owner, name, pr_number).await else {
        return Vec::new();
    };
    if !d.has_pushback() {
        info!(
            pr = pr_number,
            "Review lesson: no pushback on the review to judge"
        );
        return Vec::new();
    }
    let judged = judge(model, &d).await;
    let mut learned = Vec::new();
    for j in &judged {
        let now = chrono::Utc::now().to_rfc3339();
        let id = ulid::Ulid::new().to_string();
        let Some(new) = new_lesson(j, owner, name, pr_number, trigger, &id, &now) else {
            info!(pr = pr_number, verdict = %j.verdict, finding = %j.finding, "Review lesson: nothing to learn");
            continue;
        };
        // Compared with every lesson a review of this repository is given.
        let existing = rl::for_repo(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            owner,
            name,
        )
        .await
        .unwrap_or_default();
        let plan = plan_for(model, &existing, new.clone()).await;
        apply_plan(state, team_id, &plan).await;
        info!(
            pr = pr_number,
            scope = %new.scope,
            repeated = plan.seen_again.is_some(),
            replaced = plan.delete.len(),
            lesson = %new.lesson,
            "Reviewer learned from people's pushback"
        );
        learned.push(new.lesson);
    }
    learned
}

/// Takes the pending record from `pending` to `judging` when it is still at
/// `event_at`; false when another job has it or a newer event arrived.
async fn claim(state: &WorkerState, team_id: &str, sk: &str, event_at: i64) -> bool {
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .update_expression("SET #st = :j, judging_at = :now")
        .condition_expression("#st = :p AND last_event_at = :t")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":j", attr_s(rl::STATUS_JUDGING))
        .expression_attribute_values(":p", attr_s(rl::STATUS_PENDING))
        .expression_attribute_values(":t", crate::passes::attr_n(event_at))
        .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .is_ok()
}

/// Marks the judgment done, unless a newer event moved the record on (its
/// own job judges it then).
async fn finish(state: &WorkerState, team_id: &str, sk: &str, event_at: i64, learned: usize) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .update_expression("SET #st = :d, judged_at = :now, learned = :n")
        .condition_expression("last_event_at = :t")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":d", attr_s(rl::STATUS_JUDGED))
        .expression_attribute_values(":t", crate::passes::attr_n(event_at))
        .expression_attribute_values(":n", crate::passes::attr_n(learned))
        .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;
}

/// The PR's pending record: (status, last_event_at, last_trigger).
async fn pending(state: &WorkerState, team_id: &str, sk: &str) -> Option<(String, i64, String)> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .consistent_read(true)
        .send()
        .await
        .ok()?
        .item?;
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let at = item
        .get("last_event_at")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    Some((s("status"), at, s("last_trigger")))
}

/// A queued judgment (after the quiet period).
pub async fn run(
    state: &WorkerState,
    msg: ReviewLessonMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sk = rl::pending_sk(&msg.repo_owner, &msg.repo_name, msg.pr_number);
    let Some((status, last_event_at, _)) = pending(state, &msg.team_id, &sk).await else {
        return Ok(());
    };
    match rl::debounce(&status, last_event_at, msg.event_at) {
        rl::Debounce::Run => {}
        other => {
            info!(pr = msg.pr_number, decision = ?other, "Review lesson job skipped");
            return Ok(());
        }
    }
    if !claim(state, &msg.team_id, &sk, msg.event_at).await {
        return Ok(());
    }
    let provider = ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await?;
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let mut usage = TokenUsage::default();
    let learned = {
        let mut model = TeamModel {
            state,
            provider: &provider,
            usage: &mut usage,
        };
        learn(
            state,
            &mut model,
            &github,
            &msg.team_id,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &msg.trigger,
        )
        .await
    };
    finish(state, &msg.team_id, &sk, msg.event_at, learned.len()).await;
    Ok(())
}

/// Before a verdict review: a disputed finding on this PR whose judgment is
/// still waiting out the quiet period is judged now, so the review that
/// follows already has the lesson.
pub async fn settle_before_review(
    state: &WorkerState,
    provider: &ModelProvider,
    github: &GitHubClient,
    msg: &ReviewMessage,
    usage: &mut TokenUsage,
) {
    let sk = rl::pending_sk(&msg.repo_owner, &msg.repo_name, msg.pr_number);
    let Some((status, event_at, trigger)) = pending(state, &msg.team_id, &sk).await else {
        return;
    };
    if status != rl::STATUS_PENDING || !claim(state, &msg.team_id, &sk, event_at).await {
        return;
    }
    let mut model = TeamModel {
        state,
        provider,
        usage,
    };
    let learned = learn(
        state,
        &mut model,
        github,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
        &trigger,
    )
    .await;
    finish(state, &msg.team_id, &sk, event_at, learned.len()).await;
}

/// The lessons block for a review of `owner/name` (empty when none).
pub async fn recall(state: &WorkerState, team_id: &str, owner: &str, name: &str) -> String {
    match rl::for_repo(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        owner,
        name,
    )
    .await
    {
        Ok(l) => rl::recall_block(&l),
        Err(e) => {
            warn!(error = %e, "Could not load the reviewer's lessons");
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::VecDeque;

    /// A model that answers from a script and records what it was asked.
    #[derive(Default)]
    struct Stub {
        replies: VecDeque<Option<String>>,
        asked: Vec<(String, String)>,
    }

    #[async_trait::async_trait]
    impl Model for Stub {
        async fn ask(&mut self, system: &str, prompt: &str) -> Option<String> {
            self.asked.push((system.to_string(), prompt.to_string()));
            self.replies.pop_front().flatten()
        }
    }

    fn stub(replies: &[&str]) -> Stub {
        Stub {
            replies: replies.iter().map(|r| Some(r.to_string())).collect(),
            ..Default::default()
        }
    }

    fn user(login: &str, kind: &str) -> Value {
        json!({"login": login, "type": kind})
    }

    /// The incident: the reviewer blocked a PR that adds one id to an
    /// exclusion set the code already keeps; the author pushed back under the
    /// comment and closed the PR.
    fn incident() -> (Value, Vec<Value>, Vec<Value>, Vec<Value>) {
        let pr = json!({
            "title": "Hide program from the public listing",
            "body": "Adds one more id to HIDDEN_PROGRAM_IDS.",
            "state": "closed",
            "merged": false,
            "head": {"sha": "abc123"},
            "user": user("dev-one", "User"),
        });
        let reviews = vec![json!({
            "user": user("coderhelm[bot]", "Bot"),
            "state": "CHANGES_REQUESTED",
            "body": "One blocking finding: hardcoding a record id hides it forever.",
        })];
        let comments = vec![
            json!({
                "id": 10, "in_reply_to_id": null,
                "user": user("coderhelm[bot]", "Bot"),
                "path": "src/listing.py", "line": 12,
                "diff_hunk": "@@ -10,3 +10,4 @@\n HIDDEN_PROGRAM_IDS = {\n     \"1001\",\n+    \"2002\",\n }",
                "body": "**Blocking:** this hardcodes a record id; once the program is published it will still be hidden.",
            }),
            json!({
                "id": 11, "in_reply_to_id": 10,
                "user": user("dev-one", "User"),
                "body": "I don't want this program to show even after it changes from an unpublished state to published state",
            }),
            json!({
                "id": 12, "in_reply_to_id": 10,
                "user": user("ci-helper[bot]", "Bot"),
                "body": "Coverage unchanged.",
            }),
            json!({
                "id": 13, "in_reply_to_id": 10,
                "user": user("coderhelm[bot]", "Bot"),
                "body": "Noted.",
            }),
        ];
        (pr, reviews, comments, vec![])
    }

    const INCIDENT_REPLY: &str = "```json\n{\"judgments\":[{\"finding\":\"Hardcoded record id\",\
        \"verdict\":\"reviewer_wrong\",\"why\":\"The author wants the record hidden for good, \
        and the set exists for exactly that.\",\"lesson\":\"Adding an entry to an exclusion \
        list the code already keeps is the team's mechanism for hiding records, even after they \
        change state; don't block it, at most suggest a comment saying why the entry is there.\",\
        \"scope\":\"repo\",\"category\":\"correctness\"}]}\n```";

    #[test]
    fn only_people_count_as_replies() {
        let (pr, reviews, comments, issue) = incident();
        let d = discussion_from(&pr, &reviews, &comments, &issue);
        assert_eq!(d.findings.len(), 1);
        let f = &d.findings[0];
        assert_eq!(f.path, "src/listing.py");
        assert_eq!(f.line, 12);
        assert_eq!(
            f.replies.len(),
            1,
            "bot and CoderHelm replies are not pushback"
        );
        assert_eq!(f.replies[0].0, "dev-one");
        assert!(d.closed_unmerged && d.changes_requested && !d.dismissed);
        assert!(d.has_pushback());
    }

    #[test]
    fn a_persons_review_body_is_part_of_the_conversation() {
        let (mut pr, mut reviews, mut comments, _) = incident();
        pr["state"] = json!("open");
        comments.retain(|c| c["user"]["login"] != "dev-one");
        reviews.push(json!({"user": user("lead", "User"), "state": "COMMENTED", "body": "The reviewer is wrong: that set is how we hide programs."}));
        let d = discussion_from(&pr, &reviews, &comments, &[]);
        assert_eq!(d.conversation[0].0, "lead (in a review)");
        assert!(d.has_pushback());
        assert!(judge_prompt(&d).contains("- lead (in a review): The reviewer is wrong"));
    }

    #[test]
    fn no_human_word_and_no_close_is_nothing_to_judge() {
        let (mut pr, reviews, mut comments, _) = incident();
        pr["state"] = json!("open");
        comments.retain(|c| c["user"]["login"] != "dev-one");
        // A bot's PR comment is not pushback either.
        let issue = vec![json!({"user": user("deploy-bot", "Bot"), "body": "Preview ready"})];
        let d = discussion_from(&pr, &reviews, &comments, &issue);
        assert!(!d.has_pushback());
    }

    #[test]
    fn a_dismissal_or_a_close_over_changes_requested_is_pushback() {
        let (mut pr, mut reviews, mut comments, _) = incident();
        comments.retain(|c| c["user"]["login"] != "dev-one");
        // Closed while changes were requested.
        assert!(discussion_from(&pr, &reviews, &comments, &[]).has_pushback());
        // Closed after CoderHelm approved: nothing against the review.
        reviews
            .push(json!({"user": user("coderhelm[bot]", "Bot"), "state": "APPROVED", "body": ""}));
        assert!(!discussion_from(&pr, &reviews, &comments, &[]).has_pushback());
        // Dismissed.
        pr["state"] = json!("open");
        reviews
            .push(json!({"user": user("coderhelm[bot]", "Bot"), "state": "DISMISSED", "body": ""}));
        let d = discussion_from(&pr, &reviews, &comments, &[]);
        assert!(d.dismissed && d.has_pushback());
    }

    #[test]
    fn the_judge_sees_the_finding_the_hunk_the_code_and_the_reply() {
        let (pr, reviews, comments, issue) = incident();
        let mut d = discussion_from(&pr, &reviews, &comments, &issue);
        d.findings[0].code = "  12 | HIDDEN_PROGRAM_IDS = {".into();
        let p = judge_prompt(&d);
        assert!(p.contains("Hide program from the public listing"));
        assert!(p.contains("CoderHelm's review requests changes."));
        assert!(p.contains("closed without being merged"));
        assert!(p.contains("src/listing.py:12"));
        assert!(p.contains("+    \"2002\","));
        assert!(p.contains("12 | HIDDEN_PROGRAM_IDS"));
        assert!(p.contains("- dev-one: I don't want this program to show"));
        assert!(!p.contains("Coverage unchanged"));
    }

    #[tokio::test]
    async fn the_incident_becomes_one_general_repo_lesson() {
        let (pr, reviews, comments, issue) = incident();
        let d = discussion_from(&pr, &reviews, &comments, &issue);
        let mut model = stub(&[INCIDENT_REPLY]);
        let judged = judge(&mut model, &d).await;
        assert_eq!(judged.len(), 1);
        assert!(model.asked[0].0.contains("reviewer_wrong"));
        let l = new_lesson(
            &judged[0],
            "acme",
            "web",
            42,
            rl::TRIGGER_THREAD_REPLY,
            "01TEST",
            "2026-10-21T00:00:00Z",
        )
        .unwrap();
        assert_eq!(l.scope, "acme/web");
        assert_eq!(l.sk, "REVIEWLESSON#acme/web#01TEST");
        assert!(l.lesson.starts_with("Adding an entry to an exclusion list"));
        assert!(!l.lesson.contains("2002"), "no ids from the PR");
        // Nothing kept yet: stored without a second model call.
        let plan = plan_for(&mut model, &[], l.clone()).await;
        assert_eq!(plan.put, Some(l));
        assert_eq!(model.asked.len(), 1);
    }

    #[tokio::test]
    async fn a_thank_you_or_a_correct_finding_teaches_nothing() {
        let (mut pr, reviews, mut comments, issue) = incident();
        pr["state"] = json!("open");
        comments[1]["body"] = json!("Good catch, thanks — fixed.");
        let d = discussion_from(&pr, &reviews, &comments, &issue);
        let mut model = stub(&[
            "```json\n{\"judgments\":[{\"finding\":\"x\",\"verdict\":\"finding_correct\",\
             \"why\":\"They fixed it.\",\"lesson\":\"Keep flagging this.\",\"scope\":\"repo\"}]}\n```",
        ]);
        let judged = judge(&mut model, &d).await;
        assert_eq!(judged.len(), 1);
        assert_eq!(
            new_lesson(&judged[0], "acme", "web", 1, "t", "ID", "now"),
            None
        );
    }

    #[tokio::test]
    async fn the_judge_is_not_asked_without_pushback_and_a_bad_reply_is_ignored() {
        let mut model = stub(&["not json"]);
        assert!(judge(&mut model, &Discussion::default()).await.is_empty());
        assert!(model.asked.is_empty());
        let (pr, reviews, comments, issue) = incident();
        let d = discussion_from(&pr, &reviews, &comments, &issue);
        assert!(judge(&mut model, &d).await.is_empty());
    }

    #[tokio::test]
    async fn a_repeat_is_counted_and_a_contradiction_replaced() {
        let kept = vec![
            ReviewLesson {
                id: "A".into(),
                sk: rl::lesson_sk("acme/web", "A"),
                scope: "acme/web".into(),
                lesson: "Don't block new entries in the exclusion list.".into(),
                created_at: "2026-01-01".into(),
                ..Default::default()
            },
            ReviewLesson {
                id: "B".into(),
                sk: rl::lesson_sk(rl::TEAM_SCOPE, "B"),
                scope: rl::TEAM_SCOPE.into(),
                lesson: "Always block hardcoded ids.".into(),
                created_at: "2026-01-01".into(),
                ..Default::default()
            },
        ];
        let j = Judged {
            verdict: "reviewer_wrong".into(),
            lesson: "Entries in an existing exclusion list are fine.".into(),
            scope: "repo".into(),
            ..Default::default()
        };
        let new = new_lesson(&j, "acme", "web", 1, "t", "C", "2026-02-01").unwrap();

        let mut dup = stub(&["```json\n{\"duplicate_of\": \"A\", \"supersedes\": []}\n```"]);
        let plan = plan_for(&mut dup, &kept, new.clone()).await;
        assert_eq!(plan.put, None);
        assert_eq!(plan.seen_again.as_deref(), Some("REVIEWLESSON#acme/web#A"));
        assert!(dup.asked[0].1.contains("- id A: "));
        assert!(dup.asked[0].1.contains("## New lesson"));

        let mut sup = stub(&["{\"duplicate_of\": null, \"supersedes\": [\"B\"]}"]);
        let plan = plan_for(&mut sup, &kept, new.clone()).await;
        assert_eq!(plan.put.as_ref().map(|l| l.id.as_str()), Some("C"));
        assert_eq!(plan.delete, vec!["REVIEWLESSON#*#B".to_string()]);

        // No answer: the lesson is kept, nothing removed.
        let mut silent = Stub {
            replies: VecDeque::from(vec![None]),
            ..Default::default()
        };
        let plan = plan_for(&mut silent, &kept, new).await;
        assert!(plan.put.is_some() && plan.delete.is_empty());
    }

    #[test]
    fn a_team_scoped_lesson_holds_for_every_repository() {
        let j = Judged {
            verdict: "overstated".into(),
            lesson: "Missing docstrings are a note, never a block.".into(),
            scope: "team".into(),
            category: "docs".into(),
            ..Default::default()
        };
        let l = new_lesson(&j, "acme", "web", 3, "t", "ID", "now").unwrap();
        assert_eq!(l.scope, rl::TEAM_SCOPE);
        assert_eq!(l.source_repo, "acme/web");
    }

    #[test]
    fn the_message_round_trips_on_the_queue() {
        let m = ReviewLessonMessage {
            team_id: "TEAM#1".into(),
            installation_id: 7,
            repo_owner: "acme".into(),
            repo_name: "web".into(),
            pr_number: 42,
            trigger: rl::TRIGGER_CLOSED.into(),
            event_at: 123,
        };
        let mut v = serde_json::to_value(&m).unwrap();
        v["type"] = json!("review_lesson");
        let back: crate::models::WorkerMessage = serde_json::from_value(v).unwrap();
        assert!(matches!(
            back,
            crate::models::WorkerMessage::ReviewLesson(l) if l.pr_number == 42 && l.event_at == 123
        ));
    }
}
