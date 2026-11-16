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
//!   covering checks' results and the preview's address.
//! - [`capture`] runs once the preview is up and its end-to-end checks
//!   passed, in a step of the watch: a failed capture says why in one line
//!   and is never retried into a pass.

use aws_sdk_dynamodb::types::AttributeValue;
use base64::Engine as _;
use common::preview::{self as pv, CheckResult, Outcome, Shot, Stage, State};
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

/// A state that needs nothing more on its head.
pub fn settled(st: &State, head: &str) -> bool {
    st.head == head
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
        _ => Outcome::Running,
    }
}

/// The covering checks' newest runs on the head, and the preview address
/// any of them printed.
fn covering_results(
    runs: &serde_json::Value,
    covers: &[pv::Covering],
) -> (Vec<CheckResult>, Option<String>) {
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
    let mut out: Vec<CheckResult> = Vec::new();
    let mut url = None;
    for (name, r) in newest {
        if url.is_none() {
            url = [
                r["output"]["summary"].as_str(),
                r["output"]["text"].as_str(),
                r["details_url"].as_str(),
            ]
            .into_iter()
            .flatten()
            .find_map(pv::url_in);
        }
        out.push(CheckResult {
            name: name.to_string(),
            outcome: outcome_of(r),
            url: r["html_url"].as_str().unwrap_or("").to_string(),
        });
    }
    (out, url)
}

/// The workflow files at `git_ref`, with their text.
async fn workflows(
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
        return Some(st.clone());
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
    if !changed.iter().any(|c| pv::is_user_visible(c)) {
        st.why = "the change has no page a person sees".into();
        save(state, p, &st).await;
        return Some(st);
    }
    let tree: Vec<String> = gh
        .get_tree(p.owner, p.repo, &head)
        .await
        .map(|t| t.into_iter().map(|e| e.path).collect())
        .unwrap_or_default();
    let suite = pv::e2e_suite(&tree);
    let need = pv::e2e_need(&changed, suite.as_ref());
    if let pv::E2eNeed::Required { covered, .. } = &need {
        st.e2e_required = true;
        st.e2e_covered = *covered;
    }
    st.how_to_check = pv::how_to_check(&changed, suite.as_ref(), &pv::unit_cmd(&tree));
    let wfs = workflows(gh, p.owner, p.repo, &head).await;
    let areas = map_areas(gh, p.owner, p.repo, &head, &tree, &changed).await;
    let repo_labels: Vec<String> = gh
        .list_repo_labels(p.owner, p.repo)
        .await
        .map(|v| v.into_iter().map(|(n, _)| n).collect())
        .unwrap_or_default();
    let preferred = super::review_pr::named_labels(state, p.team_id, p.owner, p.repo).await;
    let on_pr = super::skipped_checks::labels_of(pr);
    let plan = pv::plan(&pv::PlanInput {
        workflows: &wfs,
        changed: &changed,
        base: &base,
        repo_labels: &repo_labels,
        on_pr: &on_pr,
        preferred: &preferred,
        areas: &areas,
    });
    st.covers = plan.covers.clone();
    st.draft_gated = plan.draft_gated.clone();
    if !plan.has_path() {
        st.stage = Stage::Blocked;
        st.why =
            "the repository has no preview deploy for this change, so no page could be captured"
                .into();
        save(state, p, &st).await;
        return Some(st);
    }
    // Start what the workflows need: once per label per head.
    let mut add = Vec::new();
    for l in &plan.add {
        if st.labels_added.iter().any(|a| a.eq_ignore_ascii_case(l)) {
            continue;
        }
        if super::review_pr::may_add_label(state, p.team_id, p.owner, p.repo, l).await {
            add.push(l.clone());
        }
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
                && !on_pr.iter().any(|o| o.eq_ignore_ascii_case(l))
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
    let runs = gh
        .list_check_runs_for_ref(p.owner, p.repo, &head)
        .await
        .unwrap_or_default();
    let (results, url) = covering_results(&runs, &plan.covers);
    st.checks = results;
    let triggered = !st.labels_added.is_empty() || draft;
    st.stage = pv::progress(&st.checks, triggered);
    let none_ran = st.checks.iter().all(|c| c.outcome == Outcome::Skipped);
    if none_ran && !blocked.is_empty() && st.labels_added.is_empty() && !draft {
        st.stage = Stage::Blocked;
        st.why = blocked.join("; ");
    }
    st.url = match url {
        Some(u) => u,
        None if matches!(st.stage, Stage::Ready | Stage::Deploying) => gh
            .deployment_urls(p.owner, p.repo, &head)
            .await
            .ok()
            .and_then(|d| pv::url_from_deployments(&d))
            .unwrap_or_default(),
        None => String::new(),
    };
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
        st.why = "the preview's address wasn't in its deploy's output or deployments, so its pages couldn't be captured".into();
        save(state, p, st).await;
        return;
    }
    let host = st.url["https://".len()..]
        .split('/')
        .next()
        .unwrap_or("")
        .to_string();
    let base = format!("https://{host}");
    let tree: Vec<String> = gh
        .get_tree(p.owner, p.repo, &st.head)
        .await
        .map(|t| t.into_iter().map(|e| e.path).collect())
        .unwrap_or_default();
    let changed = changed_of(gh, p).await;
    let pages = pv::routes(&tree, &changed);
    let script = pv::capture_script(&base, &pages);
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
                secret: None,
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
                common::truncate_str(o.output.lines().next().unwrap_or(""), 120)
            );
            save(state, p, st).await;
            return;
        }
        Err(e) => {
            st.why = format!("the sandbox couldn't run the capture ({e})");
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
    if let Some(why) = pv::why_no_shot(&report) {
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
    for c in report.iter().filter(|c| (1..400).contains(&c.status)) {
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
    fn covering_runs_are_read_with_the_preview_address_they_print() {
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
        let (r, url) = covering_results(&runs, &covers);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].outcome, Outcome::Passed);
        assert_eq!(r[1].outcome, Outcome::Running);
        assert_eq!(url.as_deref(), Some("https://pr-9.preview-web.example.com"));
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
        let (r, _) = covering_results(&runs, &covers);
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
}
