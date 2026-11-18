//! The preview of a pull request's head, checked the way a person checks a
//! web change: start the repository's own preview deploy and end-to-end
//! checks on it, wait for them, then capture the changed pages at desktop
//! and phone width and put them in the description.
//!
//! The rules are `common::preview`'s, read from the repository itself;
//! this module reads GitHub, adds what starts the checks, runs the capture
//! in the sandbox, keeps the images on a ref of CoderHelm's own and records
//! the [`State`] on the run (`preview_state`), where the description, the
//! ready decision, the run page and the pull request watch read it.
//!
//! - [`advance`] is called by the pull request watch on every look at a
//!   finished run's open pull request (and so for runs that finished
//!   before this existed), and by the ready decision: it adds the labels
//!   the workflows need (only labels the team's settings allow, never a
//!   shared environment's), starts a workflow that counts labels only from
//!   a push with an empty commit on CoderHelm's own branch, and reads the
//!   covering checks' results, and finds the head's preview wherever the
//!   team's host or CI publishes it (`common::preview_find`: deployments,
//!   deploy statuses, preview checks, bot comments, the description, the
//!   team's address template), waiting a bounded time while it builds.
//! - [`capture`] runs once the preview is up and its end-to-end checks
//!   passed, in a step of the watch: a failed capture says why in one line
//!   and is never retried into a pass. A protected preview is opened with
//!   the access the team saved for the repository (`common::preview_access`).

use aws_sdk_dynamodb::types::AttributeValue;
use base64::Engine as _;
use common::preview::{self as pv, CheckResult, Outcome, Shot, Stage, State};
use common::preview_access::{self as pa, PreviewAccess};
use common::preview_find::{self as pf, Answer};
use tracing::{info, warn};

use super::attr_s;
use crate::clients::github::{GitEntry, GitHubClient};
use crate::WorkerState;

/// The ref the screenshots are kept on: outside `refs/heads/`, so no
/// workflow starts on it and no branch list shows it.
pub const SHOTS_REF: &str = "refs/coderhelm/screenshots";
/// Workflow files read at most.
const MAX_WORKFLOWS: usize = 40;

/// The pull request the preview is of.
#[derive(Debug, Clone, Copy)]
pub struct Pr<'a> {
    pub team_id: &'a str,
    pub run_id: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
    pub number: u64,
}

/// The state recorded on the run.
pub async fn load(state: &WorkerState, team_id: &str, run_id: &str) -> Option<State> {
    if run_id.is_empty() {
        return None;
    }
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("#f")
        .expression_attribute_names("#f", pv::FIELD)
        .send()
        .await
        .ok()?
        .item?;
    State::from_json(item.get(pv::FIELD)?.as_s().ok()?)
}

async fn save(state: &WorkerState, p: Pr<'_>, st: &State) {
    if p.run_id.is_empty() {
        return;
    }
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(p.team_id))
        .key("run_id", attr_s(p.run_id))
        .update_expression("SET #f = :v")
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_names("#f", pv::FIELD)
        .expression_attribute_values(":v", AttributeValue::S(st.to_json()))
        .send()
        .await
    {
        warn!(run_id = p.run_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not record the preview's state");
    }
}

/// An end-to-end check failing on the head whose report the state hasn't
/// read: it is read before anything is said about it.
pub fn unread_failure(st: &State, failing: &[String]) -> bool {
    failing
        .iter()
        .filter(|f| pv::role_of(f) == pv::Role::E2e)
        .any(|f| !st.failures.iter().any(|r| &r.check == f))
}

/// A state that needs nothing more on its head.
pub fn settled(st: &State, head: &str) -> bool {
    let deploy: Vec<pv::Covering> = st
        .covers
        .iter()
        .filter(|c| !c.label.is_empty())
        .cloned()
        .collect();
    let rerunnable = st.stage == Stage::Failed
        && st.checks.iter().any(|c| {
            c.outcome == Outcome::Failed && (c.no_jobs || pv::is_covering(&deploy, &c.name))
        });
    st.head == head
        && !rerunnable
        && matches!(
            st.stage,
            Stage::NotNeeded | Stage::Checked | Stage::Blocked | Stage::Failed
        )
}

fn outcome_of(run: &serde_json::Value) -> Outcome {
    match (run["status"].as_str(), run["conclusion"].as_str()) {
        (Some("completed"), Some("success")) => Outcome::Passed,
        (Some("completed"), Some("skipped" | "neutral")) => Outcome::Skipped,
        (Some("completed"), _) => Outcome::Failed,
        // Held for a protected environment's approval.
        (Some("waiting"), _) => Outcome::Waiting,
        _ => Outcome::Running,
    }
}

/// The covering checks' newest runs on the head.
pub(crate) fn covering_results(
    runs: &serde_json::Value,
    covers: &[pv::Covering],
) -> Vec<CheckResult> {
    // The newest run of each covering check that ran: a check skipped on
    // the draft and run once it was ready reads as run, and a later event
    // that skips it (another label added) doesn't undo a run.
    let mut newest: Vec<(&str, &serde_json::Value)> = Vec::new();
    for r in runs["check_runs"].as_array().into_iter().flatten() {
        let name = r["name"].as_str().unwrap_or("");
        if !pv::is_covering(covers, name) {
            continue;
        }
        let rank = |x: &serde_json::Value| {
            (
                outcome_of(x) != Outcome::Skipped,
                x["started_at"].as_str().unwrap_or("").to_string(),
            )
        };
        match newest.iter_mut().find(|(n, _)| *n == name) {
            Some(slot) if rank(r) > rank(slot.1) => slot.1 = r,
            Some(_) => {}
            None => newest.push((name, r)),
        }
    }
    newest
        .into_iter()
        .map(|(name, r)| CheckResult {
            name: name.to_string(),
            outcome: outcome_of(r),
            url: r["html_url"].as_str().unwrap_or("").to_string(),
            environment: String::new(),
            no_jobs: false,
        })
        .collect()
}

/// The covering checks that failed in a check listing although the newest
/// check of their name doesn't say so: a later run of the same `labeled`
/// workflow, started by another label, skipped the job and hides the
/// failure from every reading that takes the newest check of each name.
/// Check names, so they compare with the base branch's checks like any
/// other. The CI fix loop counts them as failing.
pub(crate) fn hidden_failures(listing: &serde_json::Value, covers: &[pv::Covering]) -> Vec<String> {
    let newest_failed: Vec<String> = super::reviewer_guide::checks_of(listing)
        .into_iter()
        .filter(|c| c.state == super::reviewer_guide::State::Failed)
        .map(|c| c.name)
        .collect();
    covering_results(listing, covers)
        .into_iter()
        .filter(|r| r.outcome == Outcome::Failed && !newest_failed.contains(&r.name))
        .map(|r| r.name)
        .collect()
}

/// The repository's preview access with its opened secret ("" when it has
/// none). None when the team saved none.
pub(crate) async fn load_access(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
) -> Option<(PreviewAccess, String)> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&pa::sk(repo)))
        .send()
        .await
        .ok()?
        .item?;
    let a = PreviewAccess::from_item(&item)?;
    let sealed = item
        .get(pa::SECRET_ATTR)
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty());
    let secret = match sealed {
        Some(sealed) if a.kind != pa::Kind::None => {
            let kms = common::mcp_credentials::Kms {
                client: &state.kms,
                key_id: &state.config.secrets_key_id,
            };
            match pa::open(&kms, team_id, repo, sealed).await {
                Ok(t) => t,
                Err(e) => {
                    warn!(repo, error = %e, "Could not open the preview access secret");
                    String::new()
                }
            }
        }
        _ => String::new(),
    };
    Some((a, secret))
}

/// The team's preview address template for the repository: its preview
/// access, else a Tests settings test address that names the pull request.
async fn template_of(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    access: Option<&PreviewAccess>,
) -> Option<String> {
    if let Some(a) = access.map(|a| a.address.trim()).filter(|a| !a.is_empty()) {
        return Some(a.to_string());
    }
    super::ci_repro::test_address(state, team_id, repo)
        .await
        .filter(|t| pf::is_per_change(t))
}

/// Everything on the pull request that says where the head's preview is.
#[allow(clippy::too_many_arguments)]
async fn sightings(
    gh: &GitHubClient,
    p: Pr<'_>,
    pr: &serde_json::Value,
    head: &str,
    branch: &str,
    runs: &serde_json::Value,
    covers: &[pv::Covering],
    template: Option<&str>,
) -> Vec<pf::Sighting> {
    let mut all = Vec::new();
    match gh.deployments_with_statuses(p.owner, p.repo, head).await {
        Ok(d) => all.extend(pf::deployments(&d, head)),
        Err(e) => warn!(pr = p.number, error = %e, "Could not read the head's deployments"),
    }
    match gh.commit_statuses(p.owner, p.repo, head).await {
        Ok(Some(c)) => all.extend(pf::statuses(&c, head)),
        Ok(None) => {}
        Err(e) => warn!(pr = p.number, error = %e, "Could not read the head's statuses"),
    }
    all.extend(pf::check_runs(runs, head, covers));
    let head_at = gh
        .commit_time(p.owner, p.repo, head)
        .await
        .unwrap_or_default();
    match gh.list_issue_comments(p.owner, p.repo, p.number).await {
        Ok(c) => all.extend(pf::comments(&c, head, &head_at)),
        Err(e) => warn!(pr = p.number, error = %e, "Could not read the pull request's comments"),
    }
    all.extend(pf::body(pr["body"].as_str().unwrap_or(""), head));
    if let Some(url) = template.and_then(|t| {
        pf::expand(
            t,
            pf::Vars {
                pr: p.number,
                branch,
                sha: head,
                owner: p.owner,
                repo: p.repo,
            },
        )
    }) {
        all.push(pf::Sighting {
            source: pf::Source::Template,
            name: String::new(),
            url,
            ready: pf::Ready::Up,
            tied: false,
        });
    }
    all
}

/// Where the preview stands once the repository's own preview checks
/// (`from_checks`, None when no workflow covers the change) and what the
/// pull request publishes about the head's preview are put together, with
/// the wait bounded. Pure: every case is tested.
pub fn settle(st: &mut State, from_checks: Option<Stage>, answer: &Answer, now: &str) {
    let waited = pf::minutes_between(&st.waiting_since, now);
    let mut waiting_on = String::new();
    match from_checks {
        Some(stage) => st.stage = stage,
        None => st.stage = Stage::Triggered,
    }
    match answer {
        Answer::Up(f) => {
            st.url = f.url.clone();
            st.found = f.clone();
            match from_checks {
                // No workflow of the repository's covers it: the host's own
                // preview of the head is up.
                None => st.stage = Stage::Ready,
                // The workflows couldn't be started, but the head has a
                // preview: it is checked on that.
                Some(Stage::Blocked) => {
                    st.stage = Stage::Ready;
                    st.why.clear();
                }
                Some(_) => {}
            }
        }
        Answer::Building(what) => {
            waiting_on = what.clone();
            if from_checks.is_none() || st.stage == Stage::Ready {
                st.stage = Stage::Deploying;
            }
        }
        Answer::Failed(what) => {
            if from_checks.is_none()
                || matches!(st.stage, Stage::Ready | Stage::Deploying | Stage::Triggered)
            {
                st.stage = Stage::Failed;
                st.why = format!("{what} for this commit, so its pages couldn't be captured");
            }
        }
        Answer::Nothing if from_checks.is_none() => {
            if waited >= pf::GRACE_MINS {
                st.stage = Stage::Blocked;
                st.why = format!(
                    "nothing on this commit publishes a preview (no deployment, deploy status, preview check, bot comment or description link) and the team set no preview address in {}, so no page could be captured",
                    pv::ACCESS_SETTING
                );
            }
        }
        Answer::Nothing => {}
    }
    if matches!(st.stage, Stage::Triggered | Stage::Deploying) && waited >= pf::WAIT_BUDGET_MINS {
        let what = if waiting_on.is_empty() {
            "its checks hadn't finished".to_string()
        } else {
            format!("{waiting_on} was still building")
        };
        st.stage = Stage::Blocked;
        st.why = format!(
            "the preview of this commit wasn't up within {} minutes ({what}), so its pages couldn't be captured; a new push starts the wait again",
            pf::WAIT_BUDGET_MINS
        );
    }
}

/// The workflow files at `git_ref`, with their text.
pub(crate) async fn workflows(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Vec<(String, String)> {
    let Ok(entries) = gh
        .list_directory(owner, repo, ".github/workflows", git_ref)
        .await
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for e in entries
        .into_iter()
        .filter(|e| e.path.ends_with(".yml") || e.path.ends_with(".yaml"))
        .take(MAX_WORKFLOWS)
    {
        if let Ok(text) = gh.read_file(owner, repo, &e.path, git_ref).await {
            out.push((e.path, text));
        }
    }
    out
}

/// The test map's areas that cover the changed paths.
async fn map_areas(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
    tree: &[String],
    changed: &[String],
) -> Vec<String> {
    let Some(path) = common::e2e::coverage::TEST_MAP_PATHS
        .iter()
        .find(|p| tree.iter().any(|t| t == *p))
    else {
        return Vec::new();
    };
    let Some(map) = gh
        .read_file(owner, repo, path, git_ref)
        .await
        .ok()
        .and_then(|t| common::e2e::coverage::parse_test_map(&t))
    else {
        return Vec::new();
    };
    let mut out: Vec<String> = Vec::new();
    for c in changed {
        for a in map.areas_for(c) {
            if !out.contains(&a.name) {
                out.push(a.name.clone());
            }
        }
    }
    out
}

/// Pushes an empty commit on CoderHelm's own branch, so a workflow that
/// reads labels only on a push sees the labels just added. Never moves a
/// branch someone pushed to since.
async fn nudge(gh: &GitHubClient, owner: &str, repo: &str, branch: &str, head: &str) -> bool {
    let Ok((tree, _)) = gh.commit_tree_parents(owner, repo, head).await else {
        return false;
    };
    let Ok(sha) = gh
        .put_commit(
            owner,
            repo,
            "Start the preview checks with the labels they need",
            &tree,
            &[head.to_string()],
        )
        .await
    else {
        return false;
    };
    gh.fast_forward(owner, repo, branch, head, &sha)
        .await
        .unwrap_or(false)
}

/// Each failed end-to-end check on the head, read from its own report
/// before anything is said about it: the e2e workflow's comment on the
/// pull request, the check's annotations, the job's log
/// (`common::e2e::failure_read`), with whether this change caused it (the
/// same check on the base branch's head).
async fn read_failures(
    gh: &GitHubClient,
    p: Pr<'_>,
    base: &str,
    runs: &serde_json::Value,
    changed: &[String],
) -> Vec<common::e2e::failure_read::FailureRead> {
    use common::e2e::failure_read as fr;
    let failed: Vec<&serde_json::Value> = runs["check_runs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|r| outcome_of(r) == Outcome::Failed)
        .filter(|r| pv::role_of(r["name"].as_str().unwrap_or("")) == pv::Role::E2e)
        .take(3)
        .collect();
    if failed.is_empty() {
        return Vec::new();
    }
    let comments = gh
        .list_issue_comments(p.owner, p.repo, p.number)
        .await
        .unwrap_or_default();
    let base_runs = match gh.get_ref(p.owner, p.repo, base).await {
        Ok(sha) => gh
            .list_check_runs_for_ref(p.owner, p.repo, &sha)
            .await
            .unwrap_or_default(),
        Err(_) => serde_json::Value::Null,
    };
    let mut out = Vec::new();
    for r in failed {
        let name = r["name"].as_str().unwrap_or("").to_string();
        let id = r["id"].as_u64().unwrap_or(0);
        let comment = comments
            .iter()
            .rev()
            .filter(|c| c["user"]["type"].as_str() == Some("Bot"))
            .filter_map(|c| c["body"].as_str())
            .find(|b| {
                let l = b.to_ascii_lowercase();
                (l.contains(&name.to_ascii_lowercase())
                    || l.contains("e2e")
                    || l.contains("playwright")
                    || l.contains("end-to-end"))
                    && (l.contains("fail") || l.contains('✘') || l.contains('❌'))
            })
            .unwrap_or("")
            .to_string();
        let annotations = if id > 0 {
            gh.check_run_annotation_list(p.owner, p.repo, id)
                .await
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let log = if id > 0 && comment.is_empty() && annotations.is_empty() {
            gh.get_check_run_logs(p.owner, p.repo, id)
                .await
                .map(|l| {
                    let lines: Vec<&str> = l.lines().collect();
                    lines[lines.len().saturating_sub(300)..].join("\n")
                })
                .unwrap_or_default()
        } else {
            String::new()
        };
        let sources = fr::Sources {
            comment,
            annotations,
            log,
        };
        let Ok(mut read) = fr::read(&name, &sources) else {
            // Nothing at all to read: said as such, and only then.
            continue;
        };
        let base_failed = base_runs["check_runs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|b| b["name"].as_str() == Some(name.as_str()))
            .map(outcome_of)
            .find(|o| matches!(o, Outcome::Passed | Outcome::Failed))
            .map(|o| o == Outcome::Failed);
        read.cause = fr::cause(&read, changed, base_failed);
        out.push(read);
    }
    out
}

/// Reads where the preview of the pull request's head stands, starts what
/// the repository's workflows need to run it, and records it. `pr` is the
/// pull request as GitHub returns it. None when it couldn't be read.
pub async fn advance(
    state: &WorkerState,
    gh: &GitHubClient,
    p: Pr<'_>,
    pr: &serde_json::Value,
) -> Option<State> {
    let head = pr["head"]["sha"].as_str().unwrap_or("").to_string();
    let base = pr["base"]["ref"].as_str().unwrap_or("main").to_string();
    let branch = pr["head"]["ref"].as_str().unwrap_or("").to_string();
    let draft = pr["draft"].as_bool().unwrap_or(false);
    if head.is_empty() || pr["state"].as_str() != Some("open") {
        return None;
    }
    let prior = load(state, p.team_id, p.run_id).await;
    if let Some(st) = prior.as_ref().filter(|st| settled(st, &head)) {
        // Settled, unless an end-to-end check failed since and its report
        // hasn't been read.
        let runs = gh
            .list_check_runs_for_ref(p.owner, p.repo, &head)
            .await
            .unwrap_or_default();
        let failing: Vec<String> = runs["check_runs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|r| outcome_of(r) == Outcome::Failed)
            .filter_map(|r| r["name"].as_str().map(str::to_string))
            .collect();
        if !unread_failure(st, &failing) {
            return Some(st.clone());
        }
    }
    let same_head = prior.as_ref().filter(|st| st.head == head);
    let changed: Vec<String> = gh
        .compare_files_with_patches(p.owner, p.repo, &base, &head)
        .await
        .ok()?
        .into_iter()
        .filter(|(_, status, _)| status != "removed")
        .map(|(path, _, _)| path)
        .collect();
    let mut st = State {
        head: head.clone(),
        labels_added: same_head
            .map(|s| s.labels_added.clone())
            .unwrap_or_default(),
        ..Default::default()
    };
    // The CI jobs that test the changed paths, for every change: "How to
    // check" and Open items name these, never another folder's job.
    let wfs = workflows(gh, p.owner, p.repo, &head).await;
    st.ci_jobs = common::ci_cover::covering_jobs(&wfs, &changed, &base);
    let runs = gh
        .list_check_runs_for_ref(p.owner, p.repo, &head)
        .await
        .unwrap_or_default();
    st.failures = read_failures(gh, p, &base, &runs, &changed).await;
    // The deploy label's workflows cover every change, a page a person sees
    // or not (`common::deploy_label`).
    let deploy_labels = super::deploy_label::labels(state, p.team_id, p.owner, p.repo).await;
    let deploy_covers = common::deploy_label::covers(&wfs, &deploy_labels);
    let visible = changed.iter().any(|c| pv::is_user_visible(c));
    if !visible && deploy_covers.is_empty() {
        st.why = "the change has no page a person sees".into();
        save(state, p, &st).await;
        return Some(st);
    }
    let on_pr = super::skipped_checks::labels_of(pr);
    let tree: Vec<String> = if visible {
        gh.get_tree(p.owner, p.repo, &head)
            .await
            .map(|t| t.into_iter().map(|e| e.path).collect())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let mut plan = if visible {
        let suite = pv::e2e_suite(&tree);
        let need = pv::e2e_need(&changed, suite.as_ref());
        if let pv::E2eNeed::Required { covered, .. } = &need {
            st.e2e_required = true;
            st.e2e_covered = *covered;
        }
        st.how_to_check = pv::how_to_check(&changed, suite.as_ref(), &pv::unit_cmd(&tree));
        let areas = map_areas(gh, p.owner, p.repo, &head, &tree, &changed).await;
        let repo_labels: Vec<String> = gh
            .list_repo_labels(p.owner, p.repo)
            .await
            .map(|v| v.into_iter().map(|(n, _)| n).collect())
            .unwrap_or_default();
        let preferred = super::review_pr::named_labels(state, p.team_id, p.owner, p.repo).await;
        pv::plan(&pv::PlanInput {
            workflows: &wfs,
            changed: &changed,
            base: &base,
            repo_labels: &repo_labels,
            on_pr: &on_pr,
            preferred: &preferred,
            areas: &areas,
        })
    } else {
        pv::Plan::default()
    };
    plan.covers = common::deploy_label::with_covers(&plan.covers, &deploy_covers);
    for g in common::deploy_label::draft_gated(&wfs, &deploy_covers) {
        if !plan.draft_gated.contains(&g) {
            plan.draft_gated.push(g);
        }
    }
    st.covers = plan.covers.clone();
    st.draft_gated = plan.draft_gated.clone();
    let now = chrono::Utc::now().to_rfc3339();
    st.looked_at = now.clone();
    st.waiting_since = same_head
        .map(|s| s.waiting_since.clone())
        .filter(|w| !w.is_empty())
        .unwrap_or_else(|| now.clone());
    let mut from_checks = None;
    if plan.has_path() {
        let mut results = covering_results(&runs, &plan.covers);
        // Start what the workflows need: once per label per head. The
        // preview plan's labels only as the team's settings allow; the
        // settings-named deploy labels as the team named them, but never on
        // a head where their workflow already ran (the pull request's
        // opening added them).
        let added = |l: &str| st.labels_added.iter().any(|a| a.eq_ignore_ascii_case(l));
        let on = |l: &str| on_pr.iter().any(|a| a.eq_ignore_ascii_case(l));
        let mut want = Vec::new();
        for l in &plan.add {
            if !added(l)
                && super::review_pr::may_add_label(state, p.team_id, p.owner, p.repo, l).await
            {
                want.push(l.clone());
            }
        }
        for l in &deploy_labels {
            let mine: Vec<pv::Covering> = deploy_covers
                .iter()
                .filter(|c| c.label.eq_ignore_ascii_case(l))
                .cloned()
                .collect();
            let ran = results
                .iter()
                .any(|r| r.outcome != Outcome::Skipped && pv::is_covering(&mine, &r.name));
            if !mine.is_empty()
                && !ran
                && !added(l)
                && !on(l)
                && !want.iter().any(|w| w.eq_ignore_ascii_case(l))
            {
                want.push(l.clone());
            }
        }
        // A label a person or another app took off stays off, whoever's it is.
        let mut add = Vec::new();
        if !want.is_empty() {
            let events = gh.list_issue_events(p.owner, p.repo, p.number).await.ok();
            let bots = super::deploy_label::workflow_logins(gh).await;
            let bots: Vec<&str> = bots.iter().map(String::as_str).collect();
            let (put, off) = common::deploy_label::put_back(&want, events.as_ref(), &bots);
            add = put;
            st.kept_off = off;
        }
        let mut blocked = plan.blocked.clone();
        if !add.is_empty() {
            match gh.add_labels(p.owner, p.repo, p.number, &add).await {
                Ok(_) => {
                    info!(pr = p.number, labels = ?add, "Added the labels that start the preview and its end-to-end checks");
                    st.labels_added.extend(add.iter().cloned());
                    if plan.needs_push && !draft && !branch.is_empty() {
                        let pushed = nudge(gh, p.owner, p.repo, &branch, &head).await;
                        info!(
                            pr = p.number,
                            pushed, "Pushed an empty commit so the labels count"
                        );
                    }
                }
                Err(e) => {
                    warn!(pr = p.number, error = %e, "Could not add the preview labels");
                    blocked.push(format!(
                        "CoderHelm couldn't add {} ({})",
                        add.join(", "),
                        common::truncate_str(&e.to_string(), 120)
                    ));
                }
            }
        }
        let not_allowed: Vec<&String> = plan
            .add
            .iter()
            .filter(|l| {
                !st.labels_added.iter().any(|a| a.eq_ignore_ascii_case(l))
                    && !on(l)
                    && !st.kept_off.iter().any(|k| k.label.eq_ignore_ascii_case(l))
            })
            .collect();
        if !not_allowed.is_empty() && add.is_empty() {
            blocked.push(format!(
                "the team's label settings don't let CoderHelm add {}",
                not_allowed
                    .iter()
                    .map(|l| format!("`{l}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        // A check only a kept-off label starts doesn't run on this head: it
        // is said, never waited for.
        let waits: Vec<pv::Covering> = plan
            .covers
            .iter()
            .filter(|c| {
                c.label.is_empty()
                    || !st
                        .kept_off
                        .iter()
                        .any(|k| k.label.eq_ignore_ascii_case(&c.label))
            })
            .cloned()
            .collect();
        results.retain(|r| r.outcome != Outcome::Skipped || pv::is_covering(&waits, &r.name));
        for k in &st.kept_off {
            blocked.push(k.line());
        }
        // A deploy workflow that failed before any job ran has no check:
        // read from its run, so it is said rather than waited on.
        let unread: Vec<&pv::Covering> = waits
            .iter()
            .filter(|c| {
                !c.label.is_empty()
                    && !results
                        .iter()
                        .any(|r| pv::is_covering(std::slice::from_ref(*c), &r.name))
            })
            .collect();
        if !unread.is_empty() {
            results
                .extend(super::deploy_label::unstarted(gh, p.owner, p.repo, &head, &unread).await);
        }
        // A check held for an environment's approval names the environment.
        for r in results.iter_mut().filter(|r| r.outcome == Outcome::Waiting) {
            r.environment = super::deploy_label::environment_of(gh, p.owner, p.repo, &r.url).await;
        }
        st.checks = results;
        let triggered = !st.labels_added.is_empty() || draft;
        let mut stage = pv::progress(&st.checks, triggered);
        let none_ran = st.checks.iter().all(|c| c.outcome == Outcome::Skipped);
        if (none_ran && !blocked.is_empty() && st.labels_added.is_empty() && !draft)
            || waits.is_empty()
        {
            stage = Stage::Blocked;
            st.why = blocked.join("; ");
        } else if !st.kept_off.is_empty() {
            st.why = st
                .kept_off
                .iter()
                .map(common::deploy_label::KeptOff::line)
                .collect::<Vec<_>>()
                .join("; ");
        }
        from_checks = Some(stage);
    }
    if !visible {
        // No page to find or capture: the deploy label's checks are the
        // whole check.
        st.stage = match from_checks.unwrap_or_default() {
            Stage::Ready => {
                st.why = "the change has no page a person sees".into();
                Stage::Checked
            }
            s => s,
        };
        save(state, p, &st).await;
        return Some(st);
    }
    // Where the head's preview is, wherever the team's host or CI
    // publishes it.
    let repo_full = format!("{}/{}", p.owner, p.repo);
    let access = load_access(state, p.team_id, &repo_full).await;
    let template = template_of(
        state,
        p.team_id,
        &repo_full,
        access.as_ref().map(|(a, _)| a),
    )
    .await;
    let seen = sightings(
        gh,
        p,
        pr,
        &head,
        &branch,
        &runs,
        &plan.covers,
        template.as_deref(),
    )
    .await;
    let answer = pf::resolve(&seen, &changed, &pf::apps_of(&tree));
    info!(pr = p.number, sightings = seen.len(), answer = ?answer, "Read where the head's preview is");
    settle(&mut st, from_checks, &answer, &now);
    // Captured on this head already: it stays checked.
    if st.stage == Stage::Ready {
        if let Some(prev) = same_head.filter(|s| !s.shots.is_empty() || s.stage == Stage::Checked) {
            st.shots = prev.shots.clone();
            st.why = prev.why.clone();
            st.stage = Stage::Checked;
        }
    }
    save(state, p, &st).await;
    Some(st)
}

/// Captures the changed pages on the preview at desktop and phone width,
/// keeps the images on [`SHOTS_REF`] and records them. Only once the
/// preview is up and its end-to-end checks passed; whatever happens, the
/// state ends checked, with the pages or the one line saying why not.
pub async fn capture(
    state: &WorkerState,
    gh: &GitHubClient,
    p: Pr<'_>,
    st: &mut State,
    deadline: Option<std::time::Instant>,
) {
    if st.stage != Stage::Ready {
        return;
    }
    st.stage = Stage::Checked;
    if st.url.is_empty() {
        st.why = format!(
            "the preview's checks passed but nothing on this commit published its address (no deployment, deploy status, preview check, bot comment or description link) and no preview address is set in {}, so its pages couldn't be captured",
            pv::ACCESS_SETTING
        );
        save(state, p, st).await;
        return;
    }
    let host = st.url["https://".len()..]
        .split('/')
        .next()
        .unwrap_or("")
        .to_string();
    let base = pv::capture_base(&st.url);
    let repo_full = format!("{}/{}", p.owner, p.repo);
    let (kind, secret) = match load_access(state, p.team_id, &repo_full).await {
        Some((a, s)) if a.kind != pa::Kind::None && !s.is_empty() => (a.kind, s),
        _ => (pa::Kind::None, String::new()),
    };
    let hide = |t: &str| {
        if secret.is_empty() {
            t.to_string()
        } else {
            t.replace(secret.as_str(), "***")
        }
    };
    let tree: Vec<String> = gh
        .get_tree(p.owner, p.repo, &st.head)
        .await
        .map(|t| t.into_iter().map(|e| e.path).collect())
        .unwrap_or_default();
    let changed = changed_of(gh, p).await;
    let pages = pv::routes(&tree, &changed);
    let script = pv::capture_script(&base, &pages, kind);
    let source = match super::skill_run::pack(&[("capture.mjs".to_string(), script)]) {
        Ok(b) => b,
        Err(e) => {
            st.why = format!("the capture couldn't be packed ({e})");
            save(state, p, st).await;
            return;
        }
    };
    let sandbox = crate::clients::sandbox::SandboxClient::new(
        &state.codebuild,
        &state.s3,
        &state.logs,
        &state.config.sandbox_bucket_name,
        &state.config.sandbox_project_name,
    )
    .with_egress(crate::agent::policy::sandbox_egress(state, p.team_id).await);
    let key = format!(
        "{}-shots-{}",
        p.run_id,
        st.head.get(..12).unwrap_or(&st.head)
    );
    let mut hosts: Vec<String> = pv::CAPTURE_HOSTS.iter().map(|h| h.to_string()).collect();
    hosts.push(host);
    let cmd = pv::capture_command();
    let out = sandbox
        .run_script(
            crate::clients::sandbox::ScriptRun {
                key: &key,
                source,
                cmd: &cmd,
                secret: (!secret.is_empty()).then_some(secret.as_str()),
                hosts,
            },
            deadline,
        )
        .await;
    let out = match out {
        Ok(o) if o.ran => o,
        Ok(o) => {
            st.why = format!(
                "the capture didn't run in the sandbox ({})",
                common::truncate_str(&hide(o.output.lines().next().unwrap_or("")), 120)
            );
            save(state, p, st).await;
            return;
        }
        Err(e) => {
            st.why = format!(
                "the sandbox couldn't run the capture ({})",
                hide(&e.to_string())
            );
            save(state, p, st).await;
            return;
        }
    };
    let report: Vec<pv::Captured> = out
        .files
        .iter()
        .find(|(n, _)| n.ends_with("shots.json"))
        .and_then(|(_, b)| serde_json::from_str(b).ok())
        .unwrap_or_default();
    if let Some(why) = pv::why_no_shot(&report, kind != pa::Kind::None) {
        st.why = why;
        save(state, p, st).await;
        return;
    }
    match keep_images(gh, p, &st.head, &report, &out.files).await {
        Ok(shots) if !shots.is_empty() => {
            info!(
                pr = p.number,
                shots = shots.len(),
                "Captured the changed pages on the preview"
            );
            st.shots = shots;
            st.why.clear();
        }
        Ok(_) => st.why = "the capture brought back no image".into(),
        Err(e) => {
            st.why = format!(
                "the pages were captured but couldn't be kept on the repository ({})",
                common::truncate_str(&e.to_string(), 120)
            )
        }
    }
    save(state, p, st).await;
}

async fn changed_of(gh: &GitHubClient, p: Pr<'_>) -> Vec<String> {
    let Ok(pr) = gh.get_pull_request(p.owner, p.repo, p.number).await else {
        return Vec::new();
    };
    let (Some(base), Some(head)) = (pr["base"]["ref"].as_str(), pr["head"]["sha"].as_str()) else {
        return Vec::new();
    };
    gh.compare_files_with_patches(p.owner, p.repo, base, head)
        .await
        .map(|v| v.into_iter().map(|(p, _, _)| p).collect())
        .unwrap_or_default()
}

/// Writes the captured images in one commit on [`SHOTS_REF`] (on top of
/// the ones kept before) and returns each page's image address.
async fn keep_images(
    gh: &GitHubClient,
    p: Pr<'_>,
    head: &str,
    report: &[pv::Captured],
    files: &[(String, String)],
) -> Result<Vec<Shot>, Box<dyn std::error::Error + Send + Sync>> {
    let short = head.get(..12).unwrap_or(head);
    let mut entries: Vec<(String, Option<GitEntry>)> = Vec::new();
    let mut shots: Vec<(Shot, String)> = Vec::new();
    for c in report.iter().filter(|c| c.shot()) {
        let Some((_, b64)) = files
            .iter()
            .find(|(n, _)| n.ends_with(&format!("{}.png.b64", c.name)))
        else {
            continue;
        };
        let clean: String = b64.chars().filter(|c| !c.is_whitespace()).collect();
        let bytes = base64::engine::general_purpose::STANDARD.decode(clean)?;
        let path = format!("pr-{}/{short}/{}.png", p.number, c.name);
        let sha = gh.put_blob(p.owner, p.repo, &bytes).await?;
        entries.push((
            path.clone(),
            Some(GitEntry {
                mode: "100644".into(),
                kind: "blob".into(),
                sha,
            }),
        ));
        shots.push((
            Shot {
                page: c.page.clone(),
                width: c.width.clone(),
                src: String::new(),
            },
            path,
        ));
    }
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let parent = gh.own_ref(p.owner, p.repo, SHOTS_REF).await;
    let base_tree = match &parent {
        Some(c) => gh.commit_tree_parents(p.owner, p.repo, c).await?.0,
        None => String::new(),
    };
    let tree = gh.put_tree(p.owner, p.repo, &base_tree, &entries).await?;
    let commit = gh
        .put_commit(
            p.owner,
            p.repo,
            &format!("Screenshots of #{} at {short}", p.number),
            &tree,
            &parent.into_iter().collect::<Vec<_>>(),
        )
        .await?;
    gh.set_own_ref(p.owner, p.repo, SHOTS_REF, &commit).await?;
    Ok(shots
        .into_iter()
        .map(|(mut s, path)| {
            s.src = format!(
                "https://github.com/{}/{}/blob/{commit}/{path}?raw=true",
                p.owner, p.repo
            );
            s
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covering_runs_are_read_newest_first() {
        let runs = serde_json::json!({"check_runs": [
            {"name": "Deploy Preview Web App", "status": "completed", "conclusion": "success",
             "html_url": "https://github.com/o/r/runs/1",
             "output": {"summary": "Preview at https://pr-9.preview-web.example.com"}},
            {"name": "E2E on preview", "status": "in_progress", "conclusion": null,
             "html_url": "https://github.com/o/r/runs/2", "output": {}},
            {"name": "unit tests", "status": "completed", "conclusion": "success", "output": {}}
        ]});
        let covers = vec![
            pv::Covering {
                check: "Deploy Preview Web App".into(),
                ..Default::default()
            },
            pv::Covering {
                check: "E2E on preview".into(),
                ..Default::default()
            },
        ];
        let r = covering_results(&runs, &covers);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].outcome, Outcome::Passed);
        assert_eq!(r[1].outcome, Outcome::Running);
        assert_eq!(pv::progress(&r, true), Stage::Deploying);
    }

    #[test]
    fn a_check_skipped_on_the_draft_and_run_once_ready_reads_as_run() {
        let runs = serde_json::json!({"check_runs": [
            {"name": "E2E on preview", "status": "completed", "conclusion": "skipped",
             "started_at": "2026-01-01T10:00:00Z", "output": {}},
            {"name": "E2E on preview", "status": "completed", "conclusion": "success",
             "started_at": "2026-01-01T10:20:00Z", "output": {}},
            {"name": "E2E on preview", "status": "completed", "conclusion": "skipped",
             "started_at": "2026-01-01T10:40:00Z", "output": {}}
        ]});
        let covers = vec![pv::Covering {
            check: "E2E on preview".into(),
            ..Default::default()
        }];
        let r = covering_results(&runs, &covers);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].outcome, Outcome::Passed);
    }

    #[test]
    fn a_settled_head_is_not_read_again_but_a_new_head_is() {
        let st = State {
            head: "abc".into(),
            stage: Stage::Checked,
            ..Default::default()
        };
        assert!(settled(&st, "abc"));
        assert!(!settled(&st, "def"));
        let pending = State {
            head: "abc".into(),
            stage: Stage::Deploying,
            ..Default::default()
        };
        assert!(!settled(&pending, "abc"));
    }

    const T0: &str = "2026-11-17T10:00:00Z";

    fn waiting() -> State {
        State {
            head: "abc".into(),
            waiting_since: T0.into(),
            ..Default::default()
        }
    }

    fn up(source: pf::Source) -> Answer {
        Answer::Up(pf::Found {
            url: "https://pr-12.preview.example.com".into(),
            source,
            name: String::new(),
            tied: true,
        })
    }

    #[test]
    fn a_hosts_own_preview_counts_when_no_workflow_covers_the_change() {
        let mut st = waiting();
        settle(
            &mut st,
            None,
            &up(pf::Source::Deployment),
            "2026-11-17T10:03:00Z",
        );
        assert_eq!(st.stage, Stage::Ready);
        assert_eq!(st.url, "https://pr-12.preview.example.com");
        assert_eq!(st.found.source, pf::Source::Deployment);
        let mut st = waiting();
        settle(
            &mut st,
            None,
            &Answer::Building("the deployment".into()),
            "2026-11-17T10:03:00Z",
        );
        assert_eq!(st.stage, Stage::Deploying);
        assert!(st.pending());
    }

    #[test]
    fn a_preview_still_building_after_its_checks_passed_is_waited_on() {
        let mut st = waiting();
        settle(
            &mut st,
            Some(Stage::Ready),
            &Answer::Building("the deployment `pr-12`".into()),
            "2026-11-17T10:03:00Z",
        );
        assert_eq!(st.stage, Stage::Deploying);
        // The workflows couldn't start, but the head has its preview.
        let mut st = waiting();
        st.why = "the label isn't allowed".into();
        settle(
            &mut st,
            Some(Stage::Blocked),
            &up(pf::Source::Comment),
            "2026-11-17T10:03:00Z",
        );
        assert_eq!(st.stage, Stage::Ready);
        assert!(st.why.is_empty());
    }

    #[test]
    fn a_failed_head_deploy_is_said_with_where_it_was_read() {
        let mut st = waiting();
        settle(
            &mut st,
            None,
            &Answer::Failed("the head commit's deployment `Preview` failed".into()),
            "2026-11-17T10:03:00Z",
        );
        assert_eq!(st.stage, Stage::Failed);
        assert!(st.why.contains("deployment `Preview` failed"), "{}", st.why);
    }

    #[test]
    fn the_wait_is_bounded() {
        // Nothing at all on the head: a short grace, then no preview.
        let mut st = waiting();
        settle(&mut st, None, &Answer::Nothing, "2026-11-17T10:02:00Z");
        assert_eq!(st.stage, Stage::Triggered);
        let mut st = waiting();
        settle(&mut st, None, &Answer::Nothing, "2026-11-17T10:07:00Z");
        assert_eq!(st.stage, Stage::Blocked);
        assert!(st.why.contains(pv::ACCESS_SETTING), "{}", st.why);
        // Building for longer than the budget: said, never waited on for ever.
        let mut st = waiting();
        settle(
            &mut st,
            None,
            &Answer::Building("the deploy status `site/deploy-preview`".into()),
            "2026-11-17T11:00:00Z",
        );
        assert_eq!(st.stage, Stage::Blocked);
        assert!(
            st.why.contains("60 minutes") && st.why.contains("still building"),
            "{}",
            st.why
        );
        let mut st = waiting();
        settle(
            &mut st,
            Some(Stage::Deploying),
            &Answer::Nothing,
            "2026-11-17T11:05:00Z",
        );
        assert_eq!(st.stage, Stage::Blocked);
        assert!(st.why.contains("checks hadn't finished"));
    }

    #[test]
    fn a_covering_failure_a_later_skipped_run_hides_is_failing() {
        // A `labeled` workflow's deploy job failed; the review label's run of
        // the same workflow skipped the job a second later, so the newest
        // check of that name reads skipped.
        let listing = serde_json::json!({"total_count": 3, "check_runs": [
            {"name": "unit tests", "status": "completed", "conclusion": "success",
             "started_at": "2026-11-17T10:00:00Z", "output": {}},
            {"name": "Deploy app", "status": "completed", "conclusion": "failure",
             "started_at": "2026-11-17T10:05:00Z", "output": {}},
            {"name": "Deploy app", "status": "completed", "conclusion": "skipped",
             "started_at": "2026-11-17T10:05:01Z", "output": {}}
        ]});
        let covers = vec![pv::Covering {
            check: "Deploy app".into(),
            label: "CI:DEPLOY_PREVIEW".into(),
            ..Default::default()
        }];
        // By check name, so it compares with the base branch's checks.
        assert_eq!(
            hidden_failures(&listing, &covers),
            vec!["Deploy app".to_string()]
        );
        // Not a covering check: nothing.
        assert!(hidden_failures(&listing, &[]).is_empty());
        // The newest check of its name says failed already: not hidden.
        let shown = serde_json::json!({"total_count": 1, "check_runs": [
            {"name": "Deploy app", "status": "completed", "conclusion": "failure",
             "started_at": "2026-11-17T10:05:00Z", "output": {}}
        ]});
        assert!(hidden_failures(&shown, &covers).is_empty());
    }

    #[test]
    fn a_failed_deploy_label_check_is_read_again_on_the_same_head_a_failed_plan_check_is_not() {
        let failed = |name: &str, no_jobs: bool| CheckResult {
            name: name.into(),
            outcome: Outcome::Failed,
            url: String::new(),
            environment: String::new(),
            no_jobs,
        };
        let covers = vec![
            pv::Covering {
                check: "Deploy app".into(),
                workflow: ".github/workflows/deploy.yml".into(),
                label: "CI:DEPLOY_PREVIEW".into(),
                ..Default::default()
            },
            pv::Covering {
                check: "E2E on preview".into(),
                ..Default::default()
            },
        ];
        let st = |checks| State {
            head: "abc".into(),
            stage: Stage::Failed,
            covers: covers.clone(),
            checks,
            ..Default::default()
        };
        // A person can re-run either on this head and it may pass.
        assert!(!settled(&st(vec![failed("Deploy app", false)]), "abc"));
        assert!(!settled(
            &st(vec![failed("Deploy app / build", false)]),
            "abc"
        ));
        assert!(!settled(&st(vec![failed("Something else", true)]), "abc"));
        // The preview plan's own failure waits for the fix's push.
        assert!(settled(&st(vec![failed("E2E on preview", false)]), "abc"));
    }
}
