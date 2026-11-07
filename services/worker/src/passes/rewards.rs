//! The worker's side of the reward system (`common::rewards`).
//!
//! * Scores a closed CoderHelm pull request ([`score_outcome`]): merged as
//!   written or after edits (how much of the diff people changed), closed,
//!   time to merge, CI's first-try result, and review comments still open.
//! * Records review comments acted on ([`record_accepted`]) and dismissed
//!   ([`record_dismissed`]), and keeps them as examples.
//! * Gives the reviewer what it learned ([`review_learning`]): the repo's
//!   calibration, the prompt variant the bandit picks, good and avoid
//!   examples and lessons; and the coder its examples and lessons
//!   ([`coder_learning`]).
//!
//! Everything is best effort: a failure here is logged and never fails a run
//! or a review.

use crate::clients::github::{BotReviewThread, GitHubClient};
use crate::passes::review_agent::{Evidence, Finding};
use crate::WorkerState;
use common::rewards::calibration::{self, Adjustment, CategoryState};
use common::rewards::event::{self as rev, RewardEvent, RewardKind};
use common::rewards::exemplars::{self, Exemplar, Query};
use common::rewards::mapping::{self, CiSummary, RunContext};
use common::rewards::store::Store;
use common::rewards::{bandit, lessons, score, RewardOutcomeMessage};
use std::collections::{BTreeMap, HashMap};
use tracing::{info, warn};

type Item = HashMap<String, aws_sdk_dynamodb::types::AttributeValue>;

fn store(state: &WorkerState) -> Store {
    Store::new(&state.dynamo, &state.config.settings_table_name)
}

fn gs(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn gu(i: &Item, k: &str) -> u64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// The metered embedding of `text` (Titan, billed to CoderHelm, counted on
/// the team's usage). None when it can't be made: callers go without.
async fn embed(state: &WorkerState, team_id: &str, text: &str) -> Option<Vec<f32>> {
    let meter = common::usage::UsageMeter::dynamo(
        state.dynamo.clone(),
        &state.config.analytics_table_name,
        team_id,
    );
    let text = common::truncate_str(text, 2_000);
    match crate::memory::embedder::embed_async(&state.bedrock, &meter, text).await {
        Ok(v) => Some(v),
        Err(e) => {
            warn!(error = %e, "Could not embed for reward examples");
            None
        }
    }
}

/// The area most of the changed paths are in.
pub fn main_area(paths: &[String]) -> String {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for p in paths {
        let a = rev::file_area(p);
        if !a.is_empty() {
            *counts.entry(a).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
        .map(|(a, _)| a)
        .unwrap_or_default()
}

/// CI on a run, from its record (`ci_fix_attempts`, `ci_last`).
pub fn ci_of(run: &Item) -> CiSummary {
    let last = gs(run, "ci_last");
    CiSummary {
        fix_attempts: gu(run, "ci_fix_attempts"),
        passed: last == "passed",
        known: !last.is_empty() || run.contains_key("ci_fix_attempts"),
    }
}

pub use common::rewards::lessons::is_coder_setback;

/// Most of people's commits read for what they changed.
const MAX_EDIT_COMMITS: usize = 20;

/// What people changed on top of CoderHelm's work: their own non-merge
/// commits, each measured by its own stats. `None` when GitHub couldn't say
/// for any of them (then no merged-after-edits event is guessed).
async fn people_edits(
    github: &GitHubClient,
    owner: &str,
    name: &str,
    commits: &[serde_json::Value],
) -> Option<mapping::PeopleEdits> {
    let mine = common::pr_outcome::human_edit_commits(commits);
    if mine.is_empty() {
        return Some(mapping::PeopleEdits::default());
    }
    let mut lines = 0u64;
    let mut read = 0usize;
    let mut touched: BTreeMap<String, usize> = BTreeMap::new();
    let mut subjects = Vec::new();
    for c in mine.iter().take(MAX_EDIT_COMMITS) {
        let Some(sha) = c["sha"].as_str() else {
            continue;
        };
        if let Ok((n, files)) = github.commit_stats(owner, name, sha).await {
            read += 1;
            lines += n;
            for f in files {
                *touched.entry(f).or_default() += 1;
            }
        }
        let subject = common::pr_outcome::commit_subject(c);
        if !subject.is_empty() {
            subjects.push(subject);
        }
    }
    if read == 0 {
        return None;
    }
    let mut files: Vec<(String, usize)> = touched.into_iter().collect();
    files.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    Some(mapping::PeopleEdits {
        commits: mine.len() as u64,
        lines,
        files: files.into_iter().map(|(f, _)| f).collect(),
        subjects,
    })
}

/// Why people closed a run's PR, when they said: the reason picked when the
/// run was closed in CoderHelm, or their answer to CoderHelm's one question.
pub fn close_reason_of(run: &Item) -> String {
    let picked = gs(run, "close_reason");
    if !picked.trim().is_empty() {
        return picked;
    }
    gs(run, "close_reason_answer")
}

/// The coder arm a run used, when it is one of the coder's arms.
pub fn coder_arm_of(run: &Item) -> String {
    let a = gs(run, "prompt_variant");
    if bandit::arm(rev::AGENT_CODER, &a).is_some() {
        a
    } else {
        String::new()
    }
}

async fn load_run(state: &WorkerState, team_id: &str, run_id: &str) -> Option<Item> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", crate::passes::attr_s(team_id))
        .key("run_id", crate::passes::attr_s(run_id))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
}

/// The newest review record of a pull request: (sk, variant, model, prompt
/// version), so later outcomes are credited to what produced the comments.
pub async fn latest_review(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    pr: u64,
) -> Option<(String, String, String, String)> {
    let prefix = format!("REVIEW#{repo}#{pr:0>6}#");
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", crate::passes::attr_s(team_id))
        .expression_attribute_values(":p", crate::passes::attr_s(&prefix))
        .scan_index_forward(false)
        .limit(1)
        .projection_expression("sk, prompt_variant, model, prompt_version")
        .send()
        .await
        .ok()?;
    let i = out.items().first()?;
    Some((
        gs(i, "sk"),
        gs(i, "prompt_variant"),
        gs(i, "model"),
        gs(i, "prompt_version"),
    ))
}

fn credit(e: &mut RewardEvent, review: &Option<(String, String, String, String)>) {
    if let Some((sk, variant, model, pv)) = review {
        e.review_sk = sk.clone();
        e.variant = variant.clone();
        e.model = model.clone();
        e.prompt_version = pv.clone();
    }
}

/// Adds an outcome to an example, embedding the text the first time.
async fn add_example(state: &WorkerState, team_id: &str, mut ex: Exemplar, score: f64) {
    let st = store(state);
    if !st.exemplar_exists(team_id, &ex.repo, &ex.id).await {
        if let Some(v) = embed(state, team_id, &ex.text).await {
            ex.embedding = v;
        }
    }
    if let Err(e) = st.add_to_exemplar(team_id, &ex, score).await {
        warn!(error = %e, "Could not keep the reward example");
    }
}

// ── Outcomes of a closed pull request ─────────────────────────────────

pub async fn score_outcome(state: &WorkerState, msg: RewardOutcomeMessage) -> Result<(), String> {
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )
    .map_err(|e| e.to_string())?;
    let pr = github
        .get_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
        .map_err(|e| e.to_string())?;
    let closed_at = pr["closed_at"].as_str().unwrap_or("").to_string();
    let merged = pr["merged_at"].as_str().is_some_and(|m| !m.is_empty());
    let coder = !msg.review_only && !msg.run_id.is_empty();
    let mut events = Vec::new();
    let mut area = String::new();
    if coder {
        let run = load_run(state, &msg.team_id, &msg.run_id)
            .await
            .unwrap_or_default();
        let commits = github
            .list_pr_commits(&msg.repo_owner, &msg.repo_name, msg.pr_number)
            .await
            .ok();
        let human = commits
            .as_deref()
            .map(common::pr_outcome::human_commit_count);
        // What people changed after CoderHelm, from their own commits (a
        // merge of the base branch brings other work and is not an edit).
        let edits = match (human, commits.as_deref()) {
            (Some(n), Some(c)) if n > 0 => {
                people_edits(&github, &msg.repo_owner, &msg.repo_name, c).await
            }
            _ => None,
        };
        let head = pr["head"]["sha"].as_str().unwrap_or("").to_string();
        let base = pr["base"]["sha"].as_str().unwrap_or("").to_string();
        let paths = if !base.is_empty() && !head.is_empty() {
            github
                .compare_changed_files(&msg.repo_owner, &msg.repo_name, &base, &head)
                .await
                .unwrap_or_default()
        } else {
            vec![]
        };
        area = main_area(&paths);
        let ctx = RunContext {
            team_id: msg.team_id.clone(),
            run_id: msg.run_id.clone(),
            repo: repo.clone(),
            pr: msg.pr_number,
            model: gs(&run, "model"),
            prompt_version: bandit::CODER_PROMPT_VERSION.to_string(),
            // The arm the run used: its outcome is what the coder's bandit
            // learns from.
            variant: coder_arm_of(&run),
            skills: vec![],
            plugin: gs(&run, "plugin"),
            area: area.clone(),
            close_reason: close_reason_of(&run),
        };
        events.extend(mapping::pr_closed_events(
            &ctx,
            &pr,
            human,
            edits.as_ref(),
            ci_of(&run),
        ));
    }

    // What became of each of CoderHelm's review comments, notes (Nit,
    // Suggestion) as much as blockers, on its own PRs and on people's.
    if !closed_at.is_empty() {
        if let Ok(threads) = github
            .list_bot_review_threads(&msg.repo_owner, &msg.repo_name, msg.pr_number)
            .await
        {
            let review = latest_review(state, &msg.team_id, &repo, msg.pr_number).await;
            events.extend(thread_events(
                &msg.team_id,
                &repo,
                msg.pr_number,
                &threads,
                merged,
                &closed_at,
                &review,
            ));
        }
    }

    let st = store(state);
    let fresh = st.record_all(&events).await;
    let findings = events
        .iter()
        .filter(|e| e.kind.is_some_and(RewardKind::is_finding))
        .count();
    info!(
        team_id = %msg.team_id,
        pr = msg.pr_number,
        review_only = msg.review_only,
        events = events.len(),
        findings,
        fresh,
        "Pull request outcome scored"
    );
    if fresh == 0 || !coder {
        return Ok(()); // already scored (a redelivered webhook), or no coder part
    }
    note_reward_area(state, &msg.team_id, &msg.run_id, &area).await;

    // Keep the change as an example for the coder.
    let weights = st.weights(&msg.team_id).await;
    let fix_score: f64 = events
        .iter()
        .filter(|e| e.agent == rev::AGENT_CODER)
        .filter(|e| {
            matches!(
                e.kind,
                Some(RewardKind::PrMergedUnchanged)
                    | Some(RewardKind::PrMergedEdited)
                    | Some(RewardKind::PrClosedUnmerged)
            )
        })
        .map(|e| score::score(e, &weights))
        .sum();
    if fix_score != 0.0 {
        let outcome: Vec<(RewardKind, f64)> = events
            .iter()
            .filter(|e| e.agent == rev::AGENT_CODER)
            .filter_map(|e| e.kind.map(|k| (k, e.value)))
            .collect();
        let ex = Exemplar {
            id: exemplars::id_for(&repo, msg.pr_number, "fix"),
            repo: repo.clone(),
            kind: exemplars::KIND_FIX.into(),
            area: area.clone(),
            pr: msg.pr_number,
            run_id: msg.run_id.clone(),
            reason: exemplars::fix_reason(&outcome),
            ..Default::default()
        }
        .with_text(
            pr["title"].as_str().unwrap_or(""),
            common::truncate_str(pr["body"].as_str().unwrap_or(""), 4000),
        );
        add_example(state, &msg.team_id, ex, fix_score).await;
    }

    // A setback: look for a pattern in this area worth a lesson.
    if events.iter().any(is_coder_setback) {
        learn_from_setbacks(state, &msg.team_id, &repo, &area).await;
    }
    Ok(())
}

/// The calibration events of CoderHelm's review threads on a closed PR (see
/// `mapping::thread_outcome`), each credited to the review that left it.
pub fn thread_events(
    team_id: &str,
    repo: &str,
    pr: u64,
    threads: &[BotReviewThread],
    merged: bool,
    closed_at: &str,
    review: &Option<(String, String, String, String)>,
) -> Vec<RewardEvent> {
    let mut out = Vec::new();
    for t in threads {
        let Some(kind) = mapping::thread_outcome(&thread_at_close(t), merged) else {
            continue;
        };
        let mut e =
            mapping::finding_event(team_id, kind, repo, pr, &t.id, &t.path, &t.body, closed_at)
                .with_detail(&mapping::thread_outcome_detail(kind, &t.body, &t.path));
        credit(&mut e, review);
        out.push(e);
    }
    out
}

/// A thread as `mapping::thread_outcome` reads it.
pub fn thread_at_close(t: &BotReviewThread) -> mapping::ThreadAtClose {
    mapping::ThreadAtClose {
        resolved: t.resolved,
        resolved_by_person: t.resolved_by_person,
        outdated: t.outdated,
        person_replied: t.replies.iter().any(|r| !r.by_bot),
        body: t.body.clone(),
    }
}

/// Keeps the area a run's PR touched on the run (`reward_area`), so a later
/// revert of it is credited to the same area and its lesson.
async fn note_reward_area(state: &WorkerState, team_id: &str, run_id: &str, area: &str) {
    if area.is_empty() {
        return;
    }
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", crate::passes::attr_s(team_id))
        .key("run_id", crate::passes::attr_s(run_id))
        .condition_expression("attribute_exists(run_id)")
        .update_expression("SET reward_area = :a")
        .expression_attribute_values(":a", crate::passes::attr_s(area))
        .send()
        .await;
}

/// Writes (or rewrites) the coder's lesson for this area when its recent
/// setbacks back one: what people changed, why they closed or reverted it.
pub async fn learn_from_setbacks(state: &WorkerState, team_id: &str, repo: &str, area: &str) {
    let st = store(state);
    let since = (chrono::Utc::now() - chrono::Duration::days(90)).to_rfc3339();
    let Ok(events) = st.events_since(team_id, &since).await else {
        return;
    };
    let setbacks: Vec<RewardEvent> = events
        .into_iter()
        .filter(|e| e.team_id == team_id && e.repo == repo && e.area == area)
        .filter(is_coder_setback)
        .collect();
    let Some(text) = lessons::coder_lesson_text(area, &setbacks) else {
        return;
    };
    match st.learn_area(team_id, repo, area, &text, &setbacks).await {
        Ok(Some(id)) => {
            info!(team_id, repo, area, lesson = %id, "Coder lesson written or reinforced")
        }
        Ok(None) => {}
        Err(e) => warn!(error = %e, "Could not keep the coder lesson"),
    }
}

/// A person said why they closed CoderHelm's PR after it was scored (the
/// answer to its one question): the reason joins the closed event's notes
/// and the area's lesson is written again with it.
pub async fn note_close_reason(
    state: &WorkerState,
    team_id: &str,
    run: &Item,
    repo: &str,
    pr: u64,
    reason: &str,
) {
    let closed_at = gs(run, "pr_closed_at");
    if closed_at.is_empty() || reason.trim().is_empty() || pr == 0 {
        return;
    }
    let sk = rev::event_sk(&closed_at, &format!("closed#{repo}#{pr}"));
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", crate::passes::attr_s(team_id))
        .key("sk", crate::passes::attr_s(&sk))
        .condition_expression("attribute_exists(sk)")
        .update_expression("SET notes = :n")
        .expression_attribute_values(
            ":n",
            aws_sdk_dynamodb::types::AttributeValue::L(vec![crate::passes::attr_s(
                common::truncate_str(reason.trim(), 240),
            )]),
        )
        .send()
        .await;
    if r.is_err() {
        return; // never scored, or scored under another time: nothing to add to
    }
    let area = gs(run, "reward_area");
    learn_from_setbacks(state, team_id, repo, &area).await;
}

// ── Review comments acted on or dismissed ─────────────────────────────

/// Records the earlier review comments this head fixed as acted on, and
/// keeps them as good examples. `fixed` are the threads the reviewer judged
/// fixed or accepted.
pub async fn record_accepted(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    pr: u64,
    fixed: &[&BotReviewThread],
) {
    if fixed.is_empty() {
        return;
    }
    let review = latest_review(state, team_id, repo, pr).await;
    let at = chrono::Utc::now().to_rfc3339();
    let st = store(state);
    let weights = st.weights(team_id).await;
    for t in fixed {
        let mut e = mapping::finding_event(
            team_id,
            RewardKind::FindingAccepted,
            repo,
            pr,
            &t.id,
            &t.path,
            &t.body,
            &at,
        );
        credit(&mut e, &review);
        match st.record(&e).await {
            Ok(true) => {
                let ex = finding_example(repo, pr, t);
                add_example(state, team_id, ex, score::score(&e, &weights)).await;
            }
            Ok(false) => {}
            Err(err) => warn!(error = %err, "Could not record the accepted review comment"),
        }
    }
}

fn finding_example(repo: &str, pr: u64, t: &BotReviewThread) -> Exemplar {
    let body: String = t.body.lines().skip(2).collect::<Vec<_>>().join("\n");
    Exemplar {
        id: exemplars::id_for(repo, pr, &t.id),
        repo: repo.to_string(),
        kind: exemplars::KIND_FINDING.into(),
        category: mapping::thread_category(&t.body),
        area: rev::file_area(&t.path),
        pr,
        reason: exemplars::finding_reason(RewardKind::FindingAccepted),
        ..Default::default()
    }
    .with_text(
        &mapping::thread_title(&t.body),
        common::truncate_str(&body, 4000),
    )
}

/// Records a review comment the team argued with as dismissed, once the
/// judge agreed the reviewer was wrong, and keeps it as an example to avoid.
#[allow(clippy::too_many_arguments)]
pub async fn record_dismissed(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    pr: u64,
    category: &str,
    title: &str,
    why: &str,
    dedup: &str,
) {
    let review = latest_review(state, team_id, repo, pr).await;
    let mut e = RewardEvent::new(
        team_id,
        RewardKind::FindingDismissed,
        rev::AGENT_REVIEWER,
        &chrono::Utc::now().to_rfc3339(),
        &format!("finding_dismissed#{repo}#{pr}#{dedup}"),
    )
    .with_repo(repo, pr)
    .with_detail(&format!("{title}: {why}"))
    .with_source("worker:review_lesson");
    e.category = category.trim().to_ascii_lowercase();
    credit(&mut e, &review);
    let st = store(state);
    match st.record(&e).await {
        Ok(true) => {
            let weights = st.weights(team_id).await;
            let ex = Exemplar {
                id: exemplars::id_for(repo, pr, dedup),
                repo: repo.to_string(),
                kind: exemplars::KIND_FINDING.into(),
                category: e.category.clone(),
                pr,
                reason: exemplars::finding_reason(RewardKind::FindingDismissed),
                ..Default::default()
            }
            .with_text(title, why);
            add_example(state, team_id, ex, score::score(&e, &weights)).await;
        }
        Ok(false) => {}
        Err(err) => warn!(error = %err, "Could not record the dismissed review comment"),
    }
}

// ── What the agents are given ─────────────────────────────────────────

/// What the reviewer is given for one review.
#[derive(Debug, Default)]
pub struct ReviewLearning {
    /// The prompt variant chosen (recorded on the review).
    pub arm: String,
    /// Its addition to the system prompt.
    pub addendum: &'static str,
    pub calibration: Vec<CategoryState>,
    /// Calibration, examples and lessons, for the prompt.
    pub context: String,
}

/// The reviewer's learning for `repo`. `query` describes the change (title
/// and changed paths), for picking similar examples.
pub async fn review_learning(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    pr: u64,
    paths: &[String],
    query: &str,
) -> ReviewLearning {
    let st = store(state);
    let weights = st.weights(team_id).await;
    let calibration_states = st.categories(team_id, repo).await.unwrap_or_default();
    // The prompt variant: an admin's pin, else the capped bandit.
    let arms = bandit::arms_for(rev::AGENT_REVIEWER);
    let stats = st
        .arm_stats(team_id, rev::AGENT_REVIEWER, &weights)
        .await
        .unwrap_or_default();
    let pinned = st.pin(team_id, rev::AGENT_REVIEWER).await;
    let choice = bandit::choose(
        arms,
        &stats,
        pinned.as_deref(),
        bandit::MIN_PULLS,
        bandit::EXPLORATION_CAP,
    );
    st.count_pull(team_id, rev::AGENT_REVIEWER, &choice.arm)
        .await;
    let addendum = bandit::arm(rev::AGENT_REVIEWER, &choice.arm)
        .map(|a| a.addendum)
        .unwrap_or("");
    let by = exemplars::Use {
        at: chrono::Utc::now().to_rfc3339(),
        agent: rev::AGENT_REVIEWER.into(),
        run_id: String::new(),
        pr,
    };
    let examples = examples_block(
        state,
        team_id,
        repo,
        exemplars::KIND_FINDING,
        paths,
        query,
        &by,
    )
    .await;
    let now = chrono::Utc::now();
    let kept = st.lessons(team_id, Some(repo)).await.unwrap_or_default();
    let lesson_block = lessons::prompt_block(&lessons::recall(&kept, rev::AGENT_REVIEWER, now));
    let context = format!(
        "{}{examples}{lesson_block}",
        calibration::prompt_block(&calibration_states)
    );
    info!(team_id, repo, arm = %choice.arm, why = ?choice.why, "Reviewer learning loaded");
    ReviewLearning {
        arm: choice.arm,
        addendum,
        calibration: calibration_states,
        context,
    }
}

/// Notes on a review record what produced it (variant, prompt version,
/// model), so later outcomes are credited to them.
pub async fn note_review_variant(
    state: &WorkerState,
    team_id: &str,
    record_sk: &str,
    arm: &str,
    model: &str,
) {
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", crate::passes::attr_s(team_id))
        .key("sk", crate::passes::attr_s(record_sk))
        .condition_expression("attribute_exists(sk)")
        .update_expression("SET #v = :v, #p = :p, #m = :m")
        .expression_attribute_names("#v", "prompt_variant")
        .expression_attribute_names("#p", "prompt_version")
        .expression_attribute_names("#m", "model")
        .expression_attribute_values(":v", crate::passes::attr_s(arm))
        .expression_attribute_values(":p", crate::passes::attr_s(bandit::REVIEWER_PROMPT_VERSION))
        .expression_attribute_values(":m", crate::passes::attr_s(model))
        .send()
        .await;
    if let Err(e) = r {
        warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not note the review's variant");
    }
}

/// What the coder is given for one run.
#[derive(Debug, Default, Clone)]
pub struct CoderLearning {
    /// For the planner (through the triage summary): examples of merged and
    /// rejected changes, and the lessons.
    pub context: String,
    /// For the coder's own system prompt: the lessons and the variant's
    /// addendum, capped at [`MAX_CODER_PROMPT_CHARS`].
    pub implement: String,
}

/// Longest learning block in the coder's system prompt.
pub const MAX_CODER_PROMPT_CHARS: usize = 3_000;

/// The coder's block: the variant's addendum, then the lessons, capped.
pub fn implement_block(addendum: &str, lesson_block: &str) -> String {
    let text = format!("{addendum}{lesson_block}");
    common::truncate_str(&text, MAX_CODER_PROMPT_CHARS).to_string()
}

/// The coder's learning for `repo`: examples of merged and rejected changes,
/// evidence-backed lessons, and the prompt variant the coder's bandit picks.
/// A run keeps the arm it started with (continuations and retries read it
/// back from the run) and its pull is counted once.
pub async fn coder_learning(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    run_id: &str,
    query: &str,
) -> CoderLearning {
    let st = store(state);
    let run = load_run(state, team_id, run_id).await.unwrap_or_default();
    let arm = match coder_arm_of(&run) {
        a if !a.is_empty() => a,
        _ => {
            let weights = st.weights(team_id).await;
            let stats = st
                .arm_stats(team_id, rev::AGENT_CODER, &weights)
                .await
                .unwrap_or_default();
            let pinned = st.pin(team_id, rev::AGENT_CODER).await;
            let choice = bandit::choose(
                bandit::arms_for(rev::AGENT_CODER),
                &stats,
                pinned.as_deref(),
                bandit::MIN_PULLS,
                bandit::EXPLORATION_CAP,
            );
            // Only the first writer counts the pull: a continuation racing
            // its own start doesn't count twice.
            if note_coder_arm(state, team_id, run_id, &choice.arm).await {
                st.count_pull(team_id, rev::AGENT_CODER, &choice.arm).await;
                info!(team_id, repo, run_id, arm = %choice.arm, why = ?choice.why, "Coder variant chosen");
                choice.arm
            } else {
                let again = load_run(state, team_id, run_id).await.unwrap_or_default();
                match coder_arm_of(&again) {
                    a if !a.is_empty() => a,
                    _ => choice.arm,
                }
            }
        }
    };
    let addendum = bandit::arm(rev::AGENT_CODER, &arm)
        .map(|a| a.addendum)
        .unwrap_or("");
    let by = exemplars::Use {
        at: chrono::Utc::now().to_rfc3339(),
        agent: rev::AGENT_CODER.into(),
        run_id: run_id.to_string(),
        pr: 0,
    };
    let examples = examples_block(state, team_id, repo, exemplars::KIND_FIX, &[], query, &by).await;
    let kept = st.lessons(team_id, Some(repo)).await.unwrap_or_default();
    let recalled = lessons::recall(&kept, rev::AGENT_CODER, chrono::Utc::now());
    let lesson_block = lessons::prompt_block(&recalled);
    info!(
        team_id,
        repo,
        run_id,
        arm = %arm,
        lessons = recalled.len(),
        examples = !examples.is_empty(),
        "Coder learning loaded"
    );
    CoderLearning {
        implement: implement_block(addendum, &lesson_block),
        context: format!("{examples}{lesson_block}"),
    }
}

/// Notes the coder arm on the run, only when none is noted yet. True when
/// this call wrote it.
async fn note_coder_arm(state: &WorkerState, team_id: &str, run_id: &str, arm: &str) -> bool {
    if run_id.is_empty() || arm.is_empty() {
        return false;
    }
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", crate::passes::attr_s(team_id))
        .key("run_id", crate::passes::attr_s(run_id))
        .condition_expression("attribute_exists(run_id) AND attribute_not_exists(prompt_variant)")
        .update_expression("SET prompt_variant = :v, prompt_version = :p")
        .expression_attribute_values(":v", crate::passes::attr_s(arm))
        .expression_attribute_values(":p", crate::passes::attr_s(bandit::CODER_PROMPT_VERSION))
        .send()
        .await
        .is_ok()
}

async fn examples_block(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    kind: &str,
    paths: &[String],
    query: &str,
    by: &exemplars::Use,
) -> String {
    let st = store(state);
    let cands = st.exemplars(team_id, Some(repo)).await.unwrap_or_default();
    if !cands
        .iter()
        .any(|e| e.kind == kind && e.polarity().is_some())
    {
        return String::new(); // nothing to pick from: no embedding call
    }
    let emb = embed(state, team_id, query).await;
    let areas: Vec<String> = paths.iter().map(|p| rev::file_area(p)).collect();
    let (good, avoid) = exemplars::select(
        &cands,
        &Query {
            kind,
            areas: &areas,
            categories: &[],
            embedding: emb.as_deref(),
        },
    );
    let used: Vec<Exemplar> = good.iter().chain(avoid.iter()).cloned().collect();
    st.count_exemplar_use(team_id, &used, by).await;
    exemplars::prompt_block(kind, &good, &avoid)
}

/// Applies the repo's calibration to the review's findings: suppressed
/// categories are left out (a verified defect is kept as a note), downgraded
/// ones become non-blocking notes. Security and correctness categories are
/// never touched. Returns how many findings were dropped and downgraded.
pub fn apply_calibration(findings: &mut Vec<Finding>, states: &[CategoryState]) -> (usize, usize) {
    if states.is_empty() {
        return (0, 0);
    }
    let mut dropped = 0;
    let mut downgraded = 0;
    findings.retain_mut(|f| {
        let verified = matches!(f.evidence(), Evidence::VerifiedDefect);
        match calibration::finding_action(states, &f.category, verified) {
            Adjustment::Suppress => {
                dropped += 1;
                false
            }
            Adjustment::Downgrade => {
                let minor = matches!(f.severity.as_str(), "low" | "nit");
                if f.holds_merge() || !minor {
                    downgraded += 1;
                }
                f.weaken_to(Evidence::UnverifiedConcern);
                f.note_only = Some("the team keeps dismissing this category".to_string());
                if !minor {
                    f.severity = "low".into();
                }
                true
            }
            _ => true,
        }
    });
    (dropped, downgraded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::github::ThreadReply;
    use common::rewards::calibration::Counts;
    use serde_json::json;

    /// A coder run from pick to the health page: the bandit's arm is noted on
    /// the run, read back when its PR closes, carried by every outcome event,
    /// and counted by the health page as one credited PR of one expected.
    #[test]
    fn a_coder_run_feeds_its_variant_from_pick_to_health() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        use common::rewards::health;

        // 1. A fresh team: the bandit picks the default arm.
        let choice = bandit::choose(
            bandit::arms_for(rev::AGENT_CODER),
            &[],
            None,
            bandit::MIN_PULLS,
            bandit::EXPLORATION_CAP,
        );
        assert_eq!(choice.arm, "standard");

        // 2. note_coder_arm writes it on the run as `prompt_variant`.
        let mut run = Item::new();
        run.insert("prompt_variant".into(), A::S(choice.arm.clone()));
        run.insert(
            "pr_url".into(),
            A::S("https://github.com/acme/web/pull/7".into()),
        );
        run.insert("merged_at".into(), A::S("2026-11-05T10:00:00Z".into()));

        // 3. score_outcome reads it back and builds the closed-PR events.
        let arm = coder_arm_of(&run);
        assert_eq!(arm, "standard");
        let ctx = mapping::RunContext {
            team_id: "TEAM#t".into(),
            run_id: "r1".into(),
            repo: "acme/web".into(),
            pr: 7,
            prompt_version: bandit::CODER_PROMPT_VERSION.into(),
            variant: arm,
            ..Default::default()
        };
        let pr = json!({
            "number": 7, "merged_at": "2026-11-05T10:00:00Z",
            "created_at": "2026-11-05T09:00:00Z", "additions": 10, "deletions": 2
        });
        let events = mapping::pr_closed_events(&ctx, &pr, Some(0), None, Default::default());
        assert!(!events.is_empty());
        assert!(events.iter().all(|e| e.variant == "standard"), "{events:?}");

        // 4. The health page expects that PR and counts it credited.
        let closed = vec![health::ClosedPr {
            repo: "acme/web".into(),
            pr: 7,
            closed_at: "2026-11-05T10:00:00Z".into(),
            variant: "standard".into(),
        }];
        let arms = vec![health::ArmPull {
            agent: rev::AGENT_CODER.into(),
            pulls: 1,
            last_pull_at: "2026-11-05T09:00:00Z".into(),
        }];
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-06T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let loops = health::loops(
            &health::Inputs {
                events: &events,
                closed_prs: &closed,
                arms: &arms,
                ..Default::default()
            },
            now,
        );
        let v = loops.iter().find(|l| l.key == "coder_variants").unwrap();
        assert_eq!((v.count, v.expected), (1, Some(1)));
        assert_eq!(v.state, health::State::Ok);
    }

    fn finding(category: &str, evidence: &str, severity: &str) -> Finding {
        serde_json::from_value(json!({
            "file": "src/a.rs", "line": 3, "severity": severity, "category": category,
            "title": "t", "body": "b", "evidence": evidence
        }))
        .unwrap()
    }

    fn state(cat: &str, adj: Adjustment) -> CategoryState {
        CategoryState {
            category: cat.into(),
            counts: Counts {
                accepted: 0,
                dismissed: 10,
                ignored: 0,
            },
            adjustment: adj,
            exempt: calibration::is_exempt(cat),
            reverted: false,
        }
    }

    #[test]
    fn calibration_quiets_dismissed_categories_but_never_security() {
        let states = vec![
            state("style", Adjustment::Suppress),
            state("naming", Adjustment::Downgrade),
            // Even a stored suppress on security is ignored.
            state("security", Adjustment::Suppress),
        ];
        let mut f = vec![
            finding("style", "unverified-concern", "low"),
            finding("style", "verified-defect", "high"),
            finding("naming", "verified-defect", "high"),
            finding("security", "unverified-concern", "medium"),
            finding("security", "verified-defect", "blocking"),
            finding("tests", "verified-defect", "blocking"),
        ];
        let (dropped, downgraded) = apply_calibration(&mut f, &states);
        assert_eq!(dropped, 1);
        assert_eq!(downgraded, 2);
        assert_eq!(f.len(), 5);
        // The verified style defect stays, as a note.
        assert_eq!(f[0].category, "style");
        assert!(!f[0].holds_merge());
        assert_eq!(f[0].severity, "low");
        assert!(!f[1].holds_merge());
        // Security findings come through untouched.
        assert_eq!(f[2].severity, "medium");
        assert!(f[3].holds_merge());
        assert_eq!(f[3].severity, "blocking");
        // A tests finding that shows a failing test is a correctness problem.
        assert!(f[4].holds_merge());
        // Nothing to apply: nothing changes.
        let mut g = vec![finding("naming", "verified-defect", "high")];
        g[0].category = "bug".into();
        assert_eq!(apply_calibration(&mut g, &[]), (0, 0));
        assert!(g[0].holds_merge());
    }

    #[test]
    fn main_area_is_where_most_paths_are() {
        let paths: Vec<String> = ["src/api/a.ts", "src/api/b.ts", "docs/x.md", "README.md"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(main_area(&paths), "src/api");
        assert_eq!(main_area(&[]), "");
    }

    #[test]
    fn ci_is_read_from_the_run() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let run = Item::from([
            ("ci_last".to_string(), A::S("passed".into())),
            ("ci_fix_attempts".to_string(), A::N("2".into())),
        ]);
        let ci = ci_of(&run);
        assert!(ci.known && ci.passed);
        assert_eq!(ci.fix_attempts, 2);
        assert!(!ci_of(&Item::new()).known);
    }

    /// Since the reviewer approves with notes, a person merging over a Nit
    /// is no pushback to judge; what became of each note is still a signal,
    /// and it reaches calibration both ways, credited to the review's arm.
    #[test]
    fn notes_at_close_become_calibration_events() {
        let t = |id: &str, resolved, by_person, outdated, replies: Vec<ThreadReply>, body: &str| {
            BotReviewThread {
                root_id: 0,
                id: id.into(),
                path: "src/a.rs".into(),
                line: 1,
                outdated,
                body: body.into(),
                replies,
                resolved,
                resolved_by_person: by_person,
            }
        };
        let person = ThreadReply {
            author: "pat".into(),
            by_bot: false,
            body: "no".into(),
        };
        let nit = "💬 **Nit: Rename** (style, not verified)";
        let threads = vec![
            t("T1", false, false, false, vec![], nit),
            t("T2", false, false, true, vec![], nit),
            t("T3", true, true, false, vec![], nit),
            t("T4", false, false, false, vec![person], nit),
            t(
                "T5",
                false,
                false,
                false,
                vec![],
                "💬 **FYI: Cache** (other)",
            ),
        ];
        let review = Some((
            "REVIEW#acme/web#000003#t".to_string(),
            "focused".to_string(),
            "m".to_string(),
            "v".to_string(),
        ));
        let ev = thread_events(
            "TEAM#a",
            "acme/web",
            3,
            &threads,
            true,
            "2026-11-05T00:00:00Z",
            &review,
        );
        let kinds: Vec<(String, RewardKind)> = ev
            .iter()
            .map(|e| (e.dedup.clone(), e.kind.unwrap()))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("finding_ignored#T1".to_string(), RewardKind::FindingIgnored),
                (
                    "finding_accepted#T2".to_string(),
                    RewardKind::FindingAccepted
                ),
                (
                    "finding_dismissed#T3".to_string(),
                    RewardKind::FindingDismissed
                ),
            ]
        );
        assert!(ev
            .iter()
            .all(|e| e.category == "style" && e.variant == "focused"));
        assert!(ev[0]
            .detail
            .contains("still open and untouched when the PR merged"));
        // Closed without merging: untouched notes say nothing.
        let ev = thread_events(
            "TEAM#a",
            "acme/web",
            3,
            &threads,
            false,
            "2026-11-05T00:00:00Z",
            &None,
        );
        assert_eq!(ev.len(), 2);
    }

    /// The coder's lessons and its arm's addendum reach its own system
    /// prompt (not only the planner's triage summary), capped.
    #[test]
    fn the_coders_block_carries_the_arm_and_lessons_capped() {
        let addendum = bandit::arm(rev::AGENT_CODER, "read_first")
            .unwrap()
            .addendum;
        let lesson = lessons::prompt_block(&[lessons::Lesson {
            text: "In `apps/site`: on PR #4 people changed `apps/site/a.ts` before merging.".into(),
            seen: 2,
            ..Default::default()
        }]);
        let b = implement_block(addendum, &lesson);
        assert!(b.contains("## Before you write"));
        assert!(b.contains("people changed `apps/site/a.ts`"));
        let long = "x".repeat(10_000);
        assert!(implement_block(addendum, &long).len() <= MAX_CODER_PROMPT_CHARS);
        assert_eq!(implement_block("", ""), "");
    }

    #[test]
    fn a_runs_arm_and_close_reason_are_read_back() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let run = Item::from([
            ("prompt_variant".to_string(), A::S("read_first".into())),
            ("close_reason_answer".to_string(), A::S("Not needed".into())),
        ]);
        assert_eq!(coder_arm_of(&run), "read_first");
        assert_eq!(close_reason_of(&run), "Not needed");
        let other = Item::from([("prompt_variant".to_string(), A::S("focused".into()))]);
        assert_eq!(
            coder_arm_of(&other),
            "",
            "a reviewer arm is not the coder's"
        );
    }

    #[test]
    fn setbacks_are_the_bad_coder_outcomes() {
        let e = |k, v| RewardEvent::new("t", k, "coder", "2026-10-01T00:00:00Z", "d").with_value(v);
        assert!(is_coder_setback(&e(RewardKind::PrReverted, 0.0)));
        assert!(is_coder_setback(&e(RewardKind::CiFixRounds, 2.0)));
        assert!(!is_coder_setback(&e(RewardKind::CiFixRounds, 1.0)));
        assert!(is_coder_setback(&e(RewardKind::PrMergedEdited, 0.6)));
        assert!(!is_coder_setback(&e(RewardKind::PrMergedEdited, 0.1)));
        assert!(!is_coder_setback(&e(RewardKind::PrMergedUnchanged, 0.0)));
    }
}
