//! A Setup run and what Setup keeps per team.
//!
//! - `SETUP_RUN#<id>`: the run (repos, mode, choice, decisions, state);
//! - `SETUP_RUN_REPO#<id>#<repo>`: one repo's plan, its changes (with the
//!   `before` values) and its result. One record per repo, so the analysis
//!   of many repos at once never writes the same record;
//! - `SETUP_APPLIED#<repo>`: what Setup last applied (custom detection);
//! - `SETUP_SEEN#TEAM`: the settings the team has reviewed;
//! - `SETUP_SETTINGS#TEAM`: the team's Setup settings.
//!
//! Runs older than [`super::KEEP_DAYS`] are removed when the history is
//! read.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::plan::{Applied, Choice, ItemDecision, PlanItem};
use super::registry::Current;
use super::Evidence;
use crate::plugins::kv::{get_as, put_as, Expect, Kv, KvError};

pub const RUN_PREFIX: &str = "SETUP_RUN#";
pub const REPO_PREFIX: &str = "SETUP_RUN_REPO#";
pub const APPLIED_PREFIX: &str = "SETUP_APPLIED#";
pub const SEEN_SK: &str = "SETUP_SEEN#TEAM";
pub const SETTINGS_SK: &str = "SETUP_SETTINGS#TEAM";

pub fn run_sk(id: &str) -> String {
    format!("{RUN_PREFIX}{id}")
}
pub fn repo_sk(id: &str, repo: &str) -> String {
    format!("{REPO_PREFIX}{id}#{repo}")
}
pub fn applied_sk(repo: &str) -> String {
    format!("{APPLIED_PREFIX}{repo}")
}

/// How a run goes from analysis to applying.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Analyze, then wait for Review.
    #[default]
    Guided,
    /// "Do it for me": analyze and apply the recommendations at once.
    Auto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Analyzing,
    Ready,
    Applying,
    Done,
    Failed,
    Undone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepoState {
    Analyzing,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultState {
    Done,
    Failed,
    Skipped,
}

/// One run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Run {
    pub run_id: String,
    /// Set once applying starts; before that the state comes from the repos.
    #[serde(default)]
    pub state: Option<RunState>,
    pub created_at: String,
    pub by: String,
    #[serde(default)]
    pub mode: Mode,
    /// Started by itself for a newly connected repo (`auto_setup_new_repos`).
    #[serde(default)]
    pub automatic: bool,
    #[serde(default)]
    pub choice: Choice,
    #[serde(default)]
    pub only_new: bool,
    pub repos: Vec<String>,
    /// The settings this run covers.
    pub scope: Vec<String>,
    #[serde(default)]
    pub estimate_tokens: u64,
    #[serde(default)]
    pub decisions: Vec<ItemDecision>,
    #[serde(default)]
    pub applied_at: Option<String>,
    #[serde(default)]
    pub undo: Option<UndoReport>,
    /// The capabilities the person who started it lacks, with their names:
    /// a run that applies by itself never changes what they couldn't.
    #[serde(default)]
    pub lacks: std::collections::BTreeMap<String, String>,
}

impl Run {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        by: &str,
        mode: Mode,
        choice: Choice,
        only_new: bool,
        repos: Vec<String>,
        scope: Vec<String>,
        estimate_tokens: u64,
    ) -> Self {
        Run {
            run_id: crate::plugins::new_id(),
            state: None,
            created_at: crate::plugins::now_rfc3339(),
            by: by.to_string(),
            mode,
            automatic: false,
            choice,
            only_new,
            repos,
            scope,
            estimate_tokens,
            decisions: vec![],
            applied_at: None,
            undo: None,
            lacks: Default::default(),
        }
    }
}

/// One change Setup made.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Change {
    pub setting_id: String,
    pub before: Current,
    pub after: Value,
    pub at: String,
    /// The evidence behind the value, for review after the fact.
    #[serde(default)]
    pub evidence: Vec<Evidence>,
    /// Its row didn't exist before this change.
    #[serde(default)]
    pub row_created: bool,
    #[serde(default)]
    pub undone: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrRef {
    pub number: u64,
    pub url: String,
}

/// A setting that wasn't changed, and why.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemError {
    pub setting_id: String,
    pub error: String,
}

/// What applying did to one repo, in words and in counts.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    /// Settings changed.
    pub changed: usize,
    /// Settings of yours that were kept though the recommendation differs.
    pub kept: usize,
    /// Settings that act and start as a preview you can switch on.
    pub previews: usize,
    /// Settings someone chose that the repo's own facts argue against: kept,
    /// and listed first.
    #[serde(default)]
    pub worth: usize,
    /// The settings behind each count, so the lists always agree with it.
    #[serde(default)]
    pub changed_ids: Vec<String>,
    #[serde(default)]
    pub kept_ids: Vec<String>,
    #[serde(default)]
    pub preview_ids: Vec<String>,
    #[serde(default)]
    pub worth_ids: Vec<String>,
    pub text: String,
}

impl Summary {
    pub fn words(&self) -> String {
        let n = |k: usize, one: &str, many: &str| {
            if k == 1 {
                format!("1 {one}")
            } else {
                format!("{k} {many}")
            }
        };
        let mut parts = vec![if self.changed == 0 {
            "Changed nothing".to_string()
        } else {
            format!("Turned on {}", self.changed)
        }];
        if self.worth > 0 {
            parts.push(format!("{} worth a look", self.worth));
        }
        if self.previews > 0 {
            parts.push(format!(
                "{} to switch on",
                n(self.previews, "preview", "previews")
            ));
        }
        if self.kept > 0 {
            parts.push(format!(
                "kept {} someone chose",
                n(self.kept, "setting", "settings")
            ));
        }
        parts.join(", ")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepoResult {
    pub repo: String,
    pub state: ResultState,
    pub changed: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub failed: Vec<ItemError>,
    #[serde(default)]
    pub prs: Vec<PrRef>,
    #[serde(default)]
    pub summary: Summary,
}

/// One repo of a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepoDoc {
    pub run_id: String,
    pub repo: String,
    pub state: RepoState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<String>,
    #[serde(default)]
    pub items: Vec<PlanItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Something the analysis skipped (the AI read over budget).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
    #[serde(default)]
    pub tokens_used: u64,
    #[serde(default)]
    pub changes: Vec<Change>,
    #[serde(default)]
    pub result: Option<RepoResult>,
}

impl RepoDoc {
    pub fn new(run_id: &str, repo: &str) -> Self {
        Self {
            run_id: run_id.to_string(),
            repo: repo.to_string(),
            state: RepoState::Analyzing,
            progress: Some("Waiting to analyze".into()),
            items: vec![],
            error: None,
            warning: None,
            tokens_used: 0,
            changes: vec![],
            result: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Skipped {
    pub repo: String,
    pub setting_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UndoReport {
    pub restored: usize,
    pub skipped: Vec<Skipped>,
    pub at: String,
    /// Only some items were undone; the run itself stays applied.
    #[serde(default)]
    pub partial: bool,
}

/// The team's Setup settings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TeamSettings {
    #[serde(default)]
    pub auto_setup_new_repos: bool,
    #[serde(default)]
    pub updated_by: String,
    #[serde(default)]
    pub updated_at: String,
}

/// The settings the team has reviewed.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Seen {
    pub ids: BTreeSet<String>,
    #[serde(default)]
    pub at: String,
}

/// The run's state: its own once applying started, otherwise from its repos.
pub fn state_of(run: &Run, repos: &[RepoDoc]) -> RunState {
    if let Some(s) = run.state {
        return s;
    }
    if repos.iter().any(|r| r.state == RepoState::Analyzing) || repos.len() < run.repos.len() {
        return RunState::Analyzing;
    }
    if !repos.is_empty() && repos.iter().all(|r| r.state == RepoState::Failed) {
        return RunState::Failed;
    }
    RunState::Ready
}

// ── Storage ─────────────────────────────────────────────────────────────

fn kv_err(e: KvError) -> String {
    e.to_string()
}

pub async fn get_run(kv: &dyn Kv, team: &str, id: &str) -> Result<Option<(Run, u64)>, String> {
    get_as(kv, team, &run_sk(id)).await
}

pub async fn put_run(kv: &dyn Kv, team: &str, run: &Run, expect: Expect) -> Result<u64, String> {
    put_as(kv, team, &run_sk(&run.run_id), run, expect)
        .await
        .map_err(kv_err)
}

/// Changes the run with `f`, again from a fresh read if someone wrote it
/// meanwhile. `f` returns `Err` to refuse the change.
pub async fn update_run<F>(kv: &dyn Kv, team: &str, id: &str, mut f: F) -> Result<Run, String>
where
    F: FnMut(&mut Run) -> Result<(), String>,
{
    for _ in 0..5 {
        let (mut run, rev) = get_run(kv, team, id).await?.ok_or("No such run.")?;
        f(&mut run)?;
        match put_as(kv, team, &run_sk(id), &run, Expect::Rev(rev)).await {
            Ok(_) => return Ok(run),
            Err(KvError::Conflict) => continue,
            Err(e) => return Err(e.to_string()),
        }
    }
    Err("The run is being changed by someone else; try again.".into())
}

pub async fn get_repo(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    repo: &str,
) -> Result<Option<RepoDoc>, String> {
    Ok(get_as(kv, team, &repo_sk(id, repo)).await?.map(|(d, _)| d))
}

pub async fn put_repo(kv: &dyn Kv, team: &str, doc: &RepoDoc) -> Result<(), String> {
    put_as(kv, team, &repo_sk(&doc.run_id, &doc.repo), doc, Expect::Any)
        .await
        .map(|_| ())
        .map_err(kv_err)
}

/// Every repo record of a run, in the run's repo order.
pub async fn repos_of(kv: &dyn Kv, team: &str, run: &Run) -> Result<Vec<RepoDoc>, String> {
    let recs = kv
        .query(
            team,
            &format!("{REPO_PREFIX}{}#", run.run_id),
            false,
            None,
            None,
        )
        .await?;
    let mut docs: Vec<RepoDoc> = recs
        .into_iter()
        .filter_map(|r| serde_json::from_value(r.data).ok())
        .collect();
    docs.sort_by_key(|d| {
        run.repos
            .iter()
            .position(|r| *r == d.repo)
            .unwrap_or(usize::MAX)
    });
    Ok(docs)
}

/// The run history, newest first. Runs past [`super::KEEP_DAYS`] are
/// removed on the way.
pub async fn list(kv: &dyn Kv, team: &str, limit: usize) -> Result<Vec<Run>, String> {
    let recs = kv.query(team, RUN_PREFIX, true, Some(200), None).await?;
    let cutoff = chrono::Utc::now() - chrono::Duration::days(super::KEEP_DAYS);
    let mut out = Vec::new();
    for r in recs {
        let Ok(run) = serde_json::from_value::<Run>(r.data) else {
            continue;
        };
        let old = chrono::DateTime::parse_from_rfc3339(&run.created_at)
            .is_ok_and(|t| t.with_timezone(&chrono::Utc) < cutoff);
        if old {
            for d in repos_of(kv, team, &run).await.unwrap_or_default() {
                let _ = kv.delete(team, &repo_sk(&run.run_id, &d.repo)).await;
            }
            let _ = kv.delete(team, &r.sk).await;
            continue;
        }
        if out.len() < limit {
            out.push(run);
        }
    }
    Ok(out)
}

pub async fn applied(kv: &dyn Kv, team: &str, repo: &str) -> Result<Option<Applied>, String> {
    Ok(get_as(kv, team, &applied_sk(repo)).await?.map(|(a, _)| a))
}

pub async fn put_applied(kv: &dyn Kv, team: &str, repo: &str, a: &Applied) -> Result<(), String> {
    put_as(kv, team, &applied_sk(repo), a, Expect::Any)
        .await
        .map(|_| ())
        .map_err(kv_err)
}

pub async fn seen(kv: &dyn Kv, team: &str) -> Result<Option<Seen>, String> {
    Ok(get_as(kv, team, SEEN_SK).await?.map(|(s, _)| s))
}

/// Adds `ids` to what the team has reviewed.
pub async fn mark_seen(kv: &dyn Kv, team: &str, ids: &[String]) -> Result<(), String> {
    let mut s = seen(kv, team).await?.unwrap_or_default();
    s.ids.extend(ids.iter().cloned());
    s.at = crate::plugins::now_rfc3339();
    put_as(kv, team, SEEN_SK, &s, Expect::Any)
        .await
        .map(|_| ())
        .map_err(kv_err)
}

/// The registry entries the team hasn't reviewed. None before the team's
/// first Setup run: then every repo is simply not set up yet.
pub fn new_ids(seen: Option<&Seen>) -> Vec<&'static str> {
    match seen {
        None => vec![],
        Some(s) => super::registry::REGISTRY
            .iter()
            .filter(|e| !s.ids.contains(e.id))
            .map(|e| e.id)
            .collect(),
    }
}

pub async fn settings(kv: &dyn Kv, team: &str) -> Result<TeamSettings, String> {
    Ok(get_as(kv, team, SETTINGS_SK)
        .await?
        .map(|(s, _)| s)
        .unwrap_or_default())
}

pub async fn put_settings(kv: &dyn Kv, team: &str, s: &TeamSettings) -> Result<(), String> {
    put_as(kv, team, SETTINGS_SK, s, Expect::Any)
        .await
        .map(|_| ())
        .map_err(kv_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::kv::Memory;

    #[tokio::test]
    async fn new_recommendations_are_counted_from_what_was_reviewed() {
        let kv = Memory::new();
        // Never set up: nothing is "new".
        assert!(new_ids(seen(&kv, "T").await.unwrap().as_ref()).is_empty());
        let all: Vec<String> = super::super::registry::REGISTRY
            .iter()
            .map(|e| e.id.to_string())
            .collect();
        mark_seen(&kv, "T", &all[1..]).await.unwrap();
        // One entry wasn't reviewed: exactly one new item.
        assert_eq!(
            new_ids(seen(&kv, "T").await.unwrap().as_ref()),
            vec![all[0].as_str()]
        );
        mark_seen(&kv, "T", &all[..1]).await.unwrap();
        assert!(new_ids(seen(&kv, "T").await.unwrap().as_ref()).is_empty());
    }

    #[test]
    fn the_summary_reads_as_a_sentence() {
        let s = Summary {
            changed: 9,
            kept: 3,
            previews: 2,
            worth: 2,
            ..Default::default()
        };
        assert_eq!(
            s.words(),
            "Turned on 9, 2 worth a look, 2 previews to switch on, kept 3 settings someone chose"
        );
    }
}
