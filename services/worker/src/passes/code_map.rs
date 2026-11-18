//! The repository's code map in the worker (`common::code_map` has the
//! rules): brought to the run's base before the planner starts, given to
//! the planner and the coder, and what the coder still explored recorded
//! on the run.
//!
//! - [`refresh`]: compares the map's stamp with the base and rebuilds only
//!   the sections the changes since then touched (all of them for a first
//!   run), in one model call; the rest are reused.
//! - [`block`]: the stored map as a prompt section, for an agent that only
//!   reads it (the coder in every round).
//! - [`record_exploration`]: the build's tool calls before its first write,
//!   and the map it was given, on the run.

use common::code_map::{self as cm, CodeMap, Exploration, RefreshRecord, Source};
use tracing::{info, warn};

use super::attr_s;
use crate::agent::provider::{self, ModelProvider};
use crate::clients::github::GitHubClient;
use crate::models::TokenUsage;
use crate::WorkerState;

/// What [`refresh`] leaves the planner: the map as a prompt section.
pub(crate) struct ForRun {
    pub block: String,
    pub record: RefreshRecord,
}

async fn load(state: &WorkerState, team_id: &str, repo: &str) -> Option<CodeMap> {
    cm::load(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        repo,
    )
    .await
}

/// The stored map of `owner/name` as a prompt section, or empty.
pub(crate) async fn block(state: &WorkerState, team_id: &str, owner: &str, name: &str) -> String {
    load(state, team_id, &format!("{owner}/{name}"))
        .await
        .map(|m| cm::render(&m))
        .unwrap_or_default()
}

/// The commit of the stored map of `owner/name`, when there is one.
pub(crate) async fn commit(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> Option<String> {
    load(state, team_id, &format!("{owner}/{name}"))
        .await
        .filter(|m| !m.sections.is_empty())
        .map(|m| m.commit)
}

/// Brings the map of `owner/name` to the head of `base_branch` and returns
/// it as a prompt section. `None` when the base can't be read; the run goes
/// on without a map (the planner explores as before).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn refresh(
    state: &WorkerState,
    github: &GitHubClient,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    team_id: &str,
    owner: &str,
    name: &str,
    base_branch: &str,
) -> Option<ForRun> {
    let repo = format!("{owner}/{name}");
    let base = github.get_ref(owner, name, base_branch).await.ok()?;
    let tree_sha = github
        .get_tree_sha(owner, name, &base)
        .await
        .unwrap_or_default();
    let map = load(state, team_id, &repo).await;
    let same = map
        .as_ref()
        .is_some_and(|m| m.commit == base || (!tree_sha.is_empty() && m.tree == tree_sha));
    // An unchanged base with nothing behind needs no tree and no model call.
    if let Some(m) = map.as_ref().filter(|m| same && m.behind().is_empty()) {
        let r = cm::plan_refresh(Some(m), &base, &tree_sha, &m.areas(), None);
        if r.is_noop() {
            return Some(ForRun {
                block: cm::render(m),
                record: RefreshRecord {
                    commit: m.commit.clone(),
                    why: r.why,
                    reused: r.reuse,
                    ..Default::default()
                },
            });
        }
    }
    let paths: Vec<String> = match github.get_tree(owner, name, &base).await {
        Ok(t) => t
            .into_iter()
            .filter(|e| e.entry_type == "blob")
            .map(|e| e.path)
            .collect(),
        Err(e) => {
            warn!(repo, error = %e, "The code map's tree could not be read; the stored map is used as it is");
            return map.map(|m| ForRun {
                block: cm::render(&m),
                record: RefreshRecord {
                    commit: m.commit.clone(),
                    why: "tree unreadable".into(),
                    ..Default::default()
                },
            });
        }
    };
    let areas = cm::areas_of(&paths);
    let changes = match map.as_ref() {
        Some(m) if !same && !m.commit.is_empty() => github
            .get_diff(owner, name, &m.commit, &base)
            .await
            .ok()
            .and_then(|v| cm::changed_from_compare(&v)),
        _ => None,
    };
    let plan = cm::plan_refresh(map.as_ref(), &base, &tree_sha, &areas, changes.as_deref());
    let mut built: Vec<(String, String)> = Vec::new();
    if !plan.rebuild.is_empty() {
        let mut sources: Vec<Source> = Vec::new();
        for id in &plan.rebuild {
            let mut files = Vec::new();
            for path in cm::inputs_for(id, &paths) {
                if let Ok(content) = github.read_file(owner, name, &path, &base).await {
                    files.push((path, content));
                }
            }
            sources.push(Source {
                id: id.clone(),
                listing: cm::listing_for(id, &paths),
                files,
            });
        }
        match provider::converse_simple(
            state,
            provider,
            provider.primary_model_id(),
            cm::SYSTEM,
            &cm::prompt(&repo, &sources),
            usage,
        )
        .await
        {
            Ok(reply) => built = cm::parse_reply(&reply, &plan.rebuild),
            Err(e) => {
                warn!(repo, error = %e, "The code map's sections could not be written; the stored ones are kept")
            }
        }
    }
    let missed: Vec<String> = plan
        .rebuild
        .iter()
        .filter(|id| !built.iter().any(|(b, _)| b == *id))
        .cloned()
        .collect();
    let next = cm::apply(
        map.as_ref(),
        &plan,
        &built,
        &base,
        &tree_sha,
        &chrono::Utc::now().to_rfc3339(),
    );
    if next.sections.is_empty() {
        return None;
    }
    if map.as_ref() != Some(&next) {
        if let Err(e) = cm::save(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            &repo,
            &next,
        )
        .await
        {
            warn!(repo, error = %e, "The code map could not be kept");
        }
    }
    info!(
        repo,
        why = %plan.why,
        rebuilt = built.len(),
        reused = plan.reuse.len(),
        missed = missed.len(),
        "Code map brought to the base"
    );
    Some(ForRun {
        block: cm::render(&next),
        record: RefreshRecord {
            commit: base,
            why: plan.why,
            reused: plan.reuse,
            rebuilt: built.into_iter().map(|(id, _)| id).collect(),
            missed,
        },
    })
}

async fn set_run_json(state: &WorkerState, team_id: &str, run_id: &str, field: &str, json: String) {
    if run_id.is_empty() {
        return;
    }
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #f = :v")
        .expression_attribute_names("#f", field)
        .expression_attribute_values(":v", attr_s(&json))
        .send()
        .await
    {
        warn!(run_id, field, error = %e, "Could not keep the code map's record on the run");
    }
}

/// What the planner's refresh did, on the run.
pub(crate) async fn record_refresh(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    r: &RefreshRecord,
) {
    let json = serde_json::to_string(r).unwrap_or_default();
    set_run_json(state, team_id, run_id, cm::REFRESH_FIELD, json).await;
}

/// The build's exploration, on the run.
pub(crate) async fn record_exploration(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    e: &Exploration,
) {
    let json = serde_json::to_string(e).unwrap_or_default();
    set_run_json(state, team_id, run_id, cm::EXPLORATION_FIELD, json).await;
}
