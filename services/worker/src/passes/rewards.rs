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

/// The SHA of CoderHelm's last commit on a pull request (commits oldest
/// first, as GitHub lists them).
pub fn last_coderhelm_sha(commits: &[serde_json::Value]) -> Option<String> {
    commits
        .iter()
        .rev()
        .find(|c| {
            let login = c["author"]["login"]
                .as_str()
                .or(c["committer"]["login"].as_str())
                .unwrap_or("");
            login.contains("coderhelm")
        })
        .and_then(|c| c["sha"].as_str().map(str::to_string))
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

/// A coder outcome bad enough to look for a pattern behind it.
pub fn is_coder_setback(e: &RewardEvent) -> bool {
    match e.kind {
        Some(RewardKind::PrClosedUnmerged)
        | Some(RewardKind::PrReverted)
        | Some(RewardKind::IncidentLinked) => true,
        Some(RewardKind::CiFixRounds) => e.value >= 2.0,
        Some(RewardKind::PrMergedEdited) => e.value >= 0.5,
        _ => false,
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
    let run = load_run(state, &msg.team_id, &msg.run_id)
        .await
        .unwrap_or_default();
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
    let commits = github
        .list_pr_commits(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
        .ok();
    let human = commits
        .as_deref()
        .map(common::pr_outcome::human_commit_count);
    let head = pr["head"]["sha"].as_str().unwrap_or("").to_string();
    let base = pr["base"]["sha"].as_str().unwrap_or("").to_string();
    // How much of the diff people changed after CoderHelm's last commit.
    let mut edit_share = None;
    if let (Some(n), Some(c)) = (human, commits.as_deref()) {
        if n > 0 {
            if let Some(last) = last_coderhelm_sha(c) {
                if let Ok(changed) = github
                    .compare_line_changes(&msg.repo_owner, &msg.repo_name, &last, &head)
                    .await
                {
                    let total = pr["additions"].as_u64().unwrap_or(0)
                        + pr["deletions"].as_u64().unwrap_or(0);
                    edit_share = Some(mapping::edit_fraction(changed, total));
                }
            }
        }
    }
    let paths = if !base.is_empty() && !head.is_empty() {
        github
            .compare_changed_files(&msg.repo_owner, &msg.repo_name, &base, &head)
            .await
            .unwrap_or_default()
    } else {
        vec![]
    };
    let ctx = RunContext {
        team_id: msg.team_id.clone(),
        run_id: msg.run_id.clone(),
        repo: repo.clone(),
        pr: msg.pr_number,
        model: gs(&run, "model"),
        prompt_version: bandit::CODER_PROMPT_VERSION.to_string(),
        variant: String::new(),
        skills: vec![],
        plugin: gs(&run, "plugin"),
        area: main_area(&paths),
    };
    let mut events = mapping::pr_closed_events(&ctx, &pr, human, edit_share, ci_of(&run));

    // Review comments CoderHelm left that nobody acted on or answered.
    let closed_at = pr["closed_at"].as_str().unwrap_or("").to_string();
    if !closed_at.is_empty() {
        if let Ok(threads) = github
            .list_bot_review_threads(&msg.repo_owner, &msg.repo_name, msg.pr_number)
            .await
        {
            let review = latest_review(state, &msg.team_id, &repo, msg.pr_number).await;
            for t in threads.iter().filter(|t| ignored_at_close(t)) {
                let mut e = mapping::finding_event(
                    &msg.team_id,
                    RewardKind::FindingIgnored,
                    &repo,
                    msg.pr_number,
                    &t.id,
                    &t.path,
                    &t.body,
                    &closed_at,
                );
                credit(&mut e, &review);
                events.push(e);
            }
        }
    }

    let st = store(state);
    let fresh = st.record_all(&events).await;
    info!(team_id = %msg.team_id, pr = msg.pr_number, events = events.len(), fresh, "Pull request outcome scored");
    if fresh == 0 {
        return Ok(()); // already scored (a redelivered webhook)
    }

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
        let text = format!(
            "{}: {}",
            pr["title"].as_str().unwrap_or(""),
            common::truncate_str(pr["body"].as_str().unwrap_or(""), 500)
        );
        let ex = Exemplar {
            id: exemplars::id_for(&repo, msg.pr_number, "fix"),
            repo: repo.clone(),
            kind: exemplars::KIND_FIX.into(),
            area: ctx.area.clone(),
            text,
            pr: msg.pr_number,
            ..Default::default()
        };
        add_example(state, &msg.team_id, ex, fix_score).await;
    }

    // A setback: look for a pattern in this area worth a lesson.
    if events.iter().any(is_coder_setback) {
        learn_from_setbacks(state, &msg.team_id, &repo, &ctx.area).await;
    }
    Ok(())
}

/// A thread still open when the PR closed: not resolved, not answered by a
/// person, and its code not replaced.
pub fn ignored_at_close(t: &BotReviewThread) -> bool {
    !t.resolved && !t.outdated && !t.replies.iter().any(|r| !r.by_bot)
}

/// Writes (or reinforces) a coder lesson when this area's recent setbacks
/// back one.
async fn learn_from_setbacks(state: &WorkerState, team_id: &str, repo: &str, area: &str) {
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
    match st
        .learn(team_id, repo, rev::AGENT_CODER, &text, &setbacks)
        .await
    {
        Ok(Some(id)) => {
            info!(team_id, repo, area, lesson = %id, "Coder lesson written or reinforced")
        }
        Ok(None) => {}
        Err(e) => warn!(error = %e, "Could not keep the coder lesson"),
    }
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
    let body: String = t.body.lines().skip(2).collect::<Vec<_>>().join(" ");
    Exemplar {
        id: exemplars::id_for(repo, pr, &t.id),
        repo: repo.to_string(),
        kind: exemplars::KIND_FINDING.into(),
        category: mapping::thread_category(&t.body),
        area: rev::file_area(&t.path),
        text: format!(
            "{}: {}",
            mapping::thread_title(&t.body),
            common::truncate_str(&body, 400)
        ),
        pr,
        ..Default::default()
    }
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
                text: format!("{title}: {why}"),
                pr,
                ..Default::default()
            };
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
    let examples =
        examples_block(state, team_id, repo, exemplars::KIND_FINDING, paths, query).await;
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

/// The coder's learning for `repo`: examples of merged and rejected changes
/// and evidence-backed lessons. Empty when there are none.
pub async fn coder_learning(state: &WorkerState, team_id: &str, repo: &str, query: &str) -> String {
    let st = store(state);
    let examples = examples_block(state, team_id, repo, exemplars::KIND_FIX, &[], query).await;
    let kept = st.lessons(team_id, Some(repo)).await.unwrap_or_default();
    let lesson_block = lessons::prompt_block(&lessons::recall(
        &kept,
        rev::AGENT_CODER,
        chrono::Utc::now(),
    ));
    format!("{examples}{lesson_block}")
}

async fn examples_block(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    kind: &str,
    paths: &[String],
    query: &str,
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
    st.count_exemplar_use(team_id, &used).await;
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
        assert!(f[4].holds_merge());
        // Nothing to apply: nothing changes.
        let mut g = vec![finding("style", "verified-defect", "high")];
        assert_eq!(apply_calibration(&mut g, &[]), (0, 0));
        assert!(g[0].holds_merge());
    }

    #[test]
    fn last_coderhelm_commit_and_main_area() {
        let commits = vec![
            json!({"sha":"a1","author":{"login":"coderhelm[bot]"}}),
            json!({"sha":"b2","author":{"login":"coderhelm[bot]"}}),
            json!({"sha":"c3","author":{"login":"pat"}}),
        ];
        assert_eq!(last_coderhelm_sha(&commits).as_deref(), Some("b2"));
        assert_eq!(last_coderhelm_sha(&[]), None);
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

    #[test]
    fn only_untouched_threads_count_as_ignored() {
        let t = |resolved, outdated, replies: Vec<ThreadReply>| BotReviewThread {
            id: "T".into(),
            path: "a".into(),
            line: 1,
            outdated,
            body: "x".into(),
            replies,
            resolved,
            resolved_by_person: false,
        };
        assert!(ignored_at_close(&t(false, false, vec![])));
        assert!(!ignored_at_close(&t(true, false, vec![])));
        assert!(!ignored_at_close(&t(false, true, vec![])));
        let person = ThreadReply {
            author: "pat".into(),
            by_bot: false,
            body: "no".into(),
        };
        assert!(!ignored_at_close(&t(false, false, vec![person])));
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
