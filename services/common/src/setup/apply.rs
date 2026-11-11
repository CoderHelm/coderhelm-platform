//! Building a repo's plan, applying a run, and undoing it.
//!
//! Applying is idempotent: a repo already done is skipped, and a setting
//! already at its target is not written again. Each change is one audit
//! event (`setup.applied`), keeps its `before` value, and is recorded in
//! `SETUP_APPLIED#<repo>` so a later hand change shows as custom. Every
//! decision is taken again from a fresh read at apply time, so a setting
//! someone set after the analysis is still kept under "keep my settings".
//!
//! Undo puts the `before` values back, for the whole run or chosen items.
//! A setting changed since the run is left alone and listed.

use std::collections::{BTreeSet, HashMap};

use serde::Serialize;
use serde_json::Value;

use super::ai::Inferred;
use super::facts::RepoFacts;
use super::plan::{self, CurrentView, Decision, PlanItem};
use super::registry::{self, Ctx, Effect};
use super::runs::{
    self, Change, ItemError, Mode, RepoDoc, RepoResult, RepoState, ResultState, Run, RunState,
    Skipped, Summary, UndoReport,
};
use super::store::Store;
use crate::plugins::audit::{self, action, ActorKind, Event};
use crate::plugins::kv::Kv;

/// Who applies, and what they may change.
pub struct Actor<'a> {
    pub email: &'a str,
    pub kind: ActorKind,
    /// The permission a capability needs that this actor lacks, in words
    /// (`None`: they hold it).
    pub missing: &'a (dyn Fn(&str) -> Option<String> + Send + Sync),
}

/// An actor that holds every capability (the worker, applying a run an
/// admin started).
pub fn holds_all(_: &str) -> Option<String> {
    None
}

/// What the analysis may do, given the team's budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetGate {
    Proceed,
    /// Over budget in a guided run: plan from the detectors and defaults
    /// only, and say the AI read was skipped.
    SkipAi,
    /// Over budget in a run that would apply by itself: stop before
    /// anything is applied.
    Stop,
}

pub fn budget_gate(mode: Mode, over_budget: bool, ai_needed: bool) -> BudgetGate {
    match (over_budget && ai_needed, mode) {
        (false, _) => BudgetGate::Proceed,
        (true, Mode::Guided) => BudgetGate::SkipAi,
        (true, Mode::Auto) => BudgetGate::Stop,
    }
}

/// Reads every setting in the run's scope, makes the recommendations and
/// saves the repo's plan. Returns the saved record.
#[allow(clippy::too_many_arguments)]
pub async fn finish_analysis(
    store: &dyn Store,
    kv: &dyn Kv,
    run: &Run,
    facts: &RepoFacts,
    inferred: &HashMap<&'static str, Inferred>,
    tokens_used: u64,
    warning: Option<String>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<RepoDoc, String> {
    let team = store.team().to_string();
    let current = registry::read_all(store, &facts.repo).await?;
    let found = super::facts::detect(facts);
    let recs = plan::recommend(facts, &found, inferred, now);
    let applied = runs::applied(kv, &team, &facts.repo).await?;
    let seen = runs::seen(kv, &team).await?;
    let items = plan::items(
        &facts.repo,
        &run.scope,
        &current,
        &recs,
        applied.as_ref(),
        seen.as_ref().map(|s| &s.ids),
    );
    let mut doc = runs::get_repo(kv, &team, &run.run_id, &facts.repo)
        .await?
        .unwrap_or_else(|| RepoDoc::new(&run.run_id, &facts.repo));
    doc.state = RepoState::Ready;
    doc.progress = None;
    doc.items = items;
    doc.tokens_used = tokens_used;
    doc.warning = warning;
    doc.error = None;
    runs::put_repo(kv, &team, &doc).await?;
    Ok(doc)
}

/// Marks a repo's analysis failed.
pub async fn fail_analysis(kv: &dyn Kv, team: &str, run_id: &str, repo: &str, error: &str) {
    let mut doc = runs::get_repo(kv, team, run_id, repo)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| RepoDoc::new(run_id, repo));
    doc.state = RepoState::Failed;
    doc.progress = None;
    doc.error = Some(error.to_string());
    if let Err(e) = runs::put_repo(kv, team, &doc).await {
        tracing::error!(team, run_id, repo, error = %e, "Setup: could not save a failed analysis");
    }
}

fn show(v: &Value) -> String {
    match v {
        Value::String(s) if s.is_empty() => "empty".to_string(),
        Value::String(s) if s.chars().count() > 60 => {
            format!("{}...", s.chars().take(60).collect::<String>())
        }
        Value::String(s) => s.clone(),
        Value::Bool(true) => "on".to_string(),
        Value::Bool(false) => "off".to_string(),
        other => other.to_string(),
    }
}

async fn record(kv: &dyn Kv, team: &str, actor: &Actor<'_>, act: &str, run: &str, detail: String) {
    let e = Event::new(actor.email, actor.kind, act, "ok")
        .run(run)
        .detail(detail);
    audit::record(kv, team, e).await;
}

/// The previews a repo has: a setting that acts is on as a preview, or
/// recommended and waiting to be switched on.
async fn previews(store: &dyn Store, repo: &str, items: &[PlanItem]) -> Vec<String> {
    let mut n = Vec::new();
    for i in items {
        let Some(e) = registry::get(&i.setting_id).filter(|e| e.preview_only) else {
            continue;
        };
        let now = registry::read(store, e, repo).await.ok().map(|c| c.value);
        // Recommended on and still off: waiting to be switched on.
        if i.recommended == Value::Bool(true) && now.as_ref() != Some(&i.recommended) {
            n.push(e.id.to_string());
        }
    }
    for (on, preview, preview_when) in [
        ("stale_prs.enabled", "stale_prs.dry_run", true),
        ("security.enabled", "security.open_prs", false),
    ] {
        let (Some(a), Some(b)) = (registry::get(on), registry::get(preview)) else {
            continue;
        };
        let on = registry::read(store, a, repo).await.ok().map(|c| c.value);
        let pv = registry::read(store, b, repo).await.ok().map(|c| c.value);
        if on == Some(Value::Bool(true)) && pv == Some(Value::Bool(preview_when)) {
            n.push(a.id.to_string());
        }
    }
    n
}

/// The run's counts and lists for one repo, so the sentence and the lists
/// under it always agree: a setting is in exactly one list.
pub fn summarize(
    changes: &[Change],
    kept: Vec<String>,
    worth: Vec<String>,
    previews: Vec<String>,
) -> Summary {
    let mut changed: Vec<String> = Vec::new();
    for c in changes.iter().filter(|c| !c.undone) {
        // A dry run switch is the state of its feature's preview, and a
        // preview is listed with the previews.
        if c.setting_id.ends_with(".dry_run") || previews.contains(&c.setting_id) {
            continue;
        }
        if !changed.contains(&c.setting_id) {
            changed.push(c.setting_id.clone());
        }
    }
    let not_in = |l: &[String], x: &String| !l.contains(x);
    let worth: Vec<String> = worth.into_iter().filter(|x| not_in(&changed, x)).collect();
    let kept: Vec<String> = kept
        .into_iter()
        .filter(|x| not_in(&changed, x) && not_in(&worth, x) && not_in(&previews, x))
        .collect();
    let mut s = Summary {
        changed: changed.len(),
        kept: kept.len(),
        previews: previews.len(),
        worth: worth.len(),
        changed_ids: changed,
        kept_ids: kept,
        preview_ids: previews,
        worth_ids: worth,
        text: String::new(),
    };
    s.text = s.words();
    s
}

/// Applies one repo's plan. Saves the repo record and `SETUP_APPLIED`.
pub async fn apply_repo(
    store: &dyn Store,
    kv: &dyn Kv,
    run: &Run,
    doc: &mut RepoDoc,
    all_repos: &[String],
    actor: &Actor<'_>,
) -> (RepoResult, Vec<Effect>) {
    let team = store.team().to_string();
    let repo = doc.repo.clone();
    let prior = runs::applied(kv, &team, &repo).await.ok().flatten();
    let mut applied = prior.clone().unwrap_or_default();
    let mut failed: Vec<ItemError> = Vec::new();
    let mut effects = Vec::new();
    let mut changed = 0;
    let (mut kept, mut worth): (Vec<String>, Vec<String>) = (vec![], vec![]);
    let now = crate::plugins::now_rfc3339();
    let ctx = Ctx {
        repo: &repo,
        all_repos,
        by: actor.email,
        now: now.clone(),
    };
    let items = doc.items.clone();
    for item in &items {
        let Some(e) = registry::get(&item.setting_id) else {
            continue;
        };
        let cur = match registry::read(store, e, &repo).await {
            Ok(c) => c,
            Err(err) => {
                failed.push(ItemError {
                    setting_id: e.id.into(),
                    error: format!("Couldn't read it: {err}"),
                });
                continue;
            }
        };
        let fresh = PlanItem {
            current: CurrentView {
                value: cur.value.clone(),
                // What this apply has filled so far counts too.
                set: plan::is_set(e, &cur, Some(&applied)),
                deliberate: Some(plan::is_deliberate(e, &cur, Some(&applied))),
                set_by: None,
            },
            ..item.clone()
        };
        let explicit = run
            .decisions
            .iter()
            .find(|d| d.repo == repo && d.setting_id == e.id);
        let (decision, target) = plan::decide(&fresh, run.choice, explicit);
        let Some(target) = target else {
            let theirs = match run.choice {
                plan::Choice::Smart => fresh.current.deliberate(),
                _ => fresh.current.set,
            };
            if decision == Decision::Keep
                && !e.preview_only
                && theirs
                && cur.value != item.recommended
            {
                if plan::worth_a_look(&fresh) {
                    worth.push(e.id.to_string());
                } else {
                    kept.push(e.id.to_string());
                }
            }
            continue;
        };
        let target = match registry::check(e, &target) {
            Ok(t) => t,
            Err(m) => {
                failed.push(ItemError {
                    setting_id: e.id.into(),
                    error: m,
                });
                continue;
            }
        };
        if let Some(need) = (actor.missing)(e.capability) {
            failed.push(ItemError {
                setting_id: e.id.into(),
                error: format!("Needs the \"{need}\" permission"),
            });
            continue;
        }
        if cur.value == target {
            applied.values.insert(e.id.into(), target);
            continue;
        }
        match registry::write(store, e, &ctx, &target).await {
            Ok(fx) => effects.extend(fx),
            Err(err) => {
                failed.push(ItemError {
                    setting_id: e.id.into(),
                    error: err.to_string(),
                });
                continue;
            }
        }
        if !cur.row {
            for other in registry::same_row(e) {
                if other.id != e.id && !applied.values.contains_key(other.id) {
                    applied.filled.insert(other.id.into());
                }
            }
        }
        applied.filled.remove(e.id);
        applied.values.insert(e.id.into(), target.clone());
        // A value a person picked in Review is theirs from now on.
        if decision == Decision::Edit
            || explicit.is_some_and(|d| d.decision == Decision::Use && target != item.recommended)
        {
            applied.chosen.insert(e.id.into());
        } else {
            applied.chosen.remove(e.id);
        }
        record(
            kv,
            &team,
            actor,
            action::SETUP_APPLIED,
            &run.run_id,
            format!(
                "{repo}: {} set from {} to {}",
                e.title,
                show(&cur.value),
                show(&target)
            ),
        )
        .await;
        // A row an earlier change of this run created reads as existing
        // here, so only the first change into a new row is marked.
        let row_created = !cur.row;
        doc.changes.push(Change {
            setting_id: e.id.into(),
            before: cur,
            after: target,
            at: now.clone(),
            evidence: item.evidence.clone(),
            row_created,
            undone: false,
        });
        changed += 1;
    }
    applied.run_id = run.run_id.clone();
    applied.at = now;
    if let Err(err) = runs::put_applied(kv, &team, &repo, &applied).await {
        failed.push(ItemError {
            setting_id: String::new(),
            error: format!("Couldn't record what was applied: {err}"),
        });
    }
    let summary = summarize(
        &doc.changes,
        kept,
        worth,
        previews(store, &repo, &items).await,
    );
    let result = RepoResult {
        repo: repo.clone(),
        state: if failed.is_empty() {
            ResultState::Done
        } else {
            ResultState::Failed
        },
        changed,
        error: (!failed.is_empty()).then(|| {
            failed
                .iter()
                .map(|f| {
                    let title = registry::get(&f.setting_id).map_or("", |e| e.title);
                    if title.is_empty() {
                        f.error.clone()
                    } else {
                        format!("{title}: {}", f.error)
                    }
                })
                .collect::<Vec<_>>()
                .join("; ")
        }),
        failed,
        prs: vec![],
        summary,
    };
    // Changes from earlier attempts are kept: undo covers them too.
    doc.result = Some(result.clone());
    if let Err(err) = runs::put_repo(kv, &team, doc).await {
        tracing::error!(team, repo, error = %err, "Setup: could not save a repo's result");
    }
    (result, effects)
}

/// Writes one setting of one repo of a run to `target` after the run was
/// applied (Switch on, Use recommended): the same validation, one audit
/// event, and a change with its `before` value, so single-item undo puts it
/// back. `None` when it already has that value.
#[allow(clippy::too_many_arguments)]
async fn change_one(
    store: &dyn Store,
    kv: &dyn Kv,
    run_id: &str,
    doc: &mut RepoDoc,
    e: &'static registry::Entry,
    target: Value,
    verb: &str,
    all_repos: &[String],
    actor: &Actor<'_>,
) -> Result<Option<Change>, String> {
    let team = store.team().to_string();
    let repo = doc.repo.clone();
    if let Some(need) = (actor.missing)(e.capability) {
        return Err(format!("Needs the \"{need}\" permission"));
    }
    let target = registry::check(e, &target)?;
    let cur = registry::read(store, e, &repo).await?;
    if cur.value == target {
        return Ok(None);
    }
    let now = crate::plugins::now_rfc3339();
    let ctx = Ctx {
        repo: &repo,
        all_repos,
        by: actor.email,
        now: now.clone(),
    };
    registry::write(store, e, &ctx, &target)
        .await
        .map_err(|x| x.to_string())?;
    record(
        kv,
        &team,
        actor,
        action::SETUP_APPLIED,
        run_id,
        format!(
            "{repo}: {} {verb}, from {} to {}",
            e.title,
            show(&cur.value),
            show(&target)
        ),
    )
    .await;
    let evidence = doc
        .items
        .iter()
        .find(|i| i.setting_id == e.id)
        .map(|i| i.evidence.clone())
        .unwrap_or_default();
    let change = Change {
        setting_id: e.id.into(),
        row_created: !cur.row,
        before: cur,
        after: target.clone(),
        at: now.clone(),
        evidence,
        undone: false,
    };
    doc.changes.push(change.clone());
    let mut applied = runs::applied(kv, &team, &repo).await?.unwrap_or_default();
    applied.values.insert(e.id.into(), target);
    applied.filled.remove(e.id);
    applied.chosen.remove(e.id);
    applied.at = now;
    runs::put_applied(kv, &team, &repo, &applied).await?;
    if let Some(r) = doc.result.as_mut() {
        r.changed += 1;
    }
    Ok(Some(change))
}

/// The repo's counts again after a change made from its result.
async fn resummarize(store: &dyn Store, doc: &mut RepoDoc) {
    let Some(r) = doc.result.as_ref() else {
        return;
    };
    let old = r.summary.clone();
    let previews = previews(store, &doc.repo, &doc.items).await;
    let s = summarize(&doc.changes, old.kept_ids, old.worth_ids, previews);
    if let Some(r) = doc.result.as_mut() {
        r.summary = s;
    }
}

fn applied_run(run: &Run) -> Result<(), String> {
    if run.state == Some(RunState::Undone) {
        return Err("This run was undone.".into());
    }
    Ok(())
}

/// Turns one repo's preview into the real setting (stale cleanup leaves its
/// dry run, security fixes open pull requests, auto-merge goes on), through
/// the same validation, audit and `before` record as applying, so undo puts
/// it back. Returns the change, `None` when it was on already.
pub async fn switch_on(
    store: &dyn Store,
    kv: &dyn Kv,
    run_id: &str,
    repo: &str,
    setting_id: &str,
    all_repos: &[String],
    actor: &Actor<'_>,
) -> Result<Option<Change>, String> {
    let team = store.team().to_string();
    let (run, _) = runs::get_run(kv, &team, run_id)
        .await?
        .ok_or("No such run.")?;
    applied_run(&run)?;
    let mut doc = runs::get_repo(kv, &team, run_id, repo)
        .await?
        .ok_or("That repo isn't in this run.")?;
    let (e, target) = registry::switch_on_target(setting_id)
        .ok_or("That setting has no preview to switch on.")?;
    let change = change_one(
        store,
        kv,
        run_id,
        &mut doc,
        e,
        target,
        "switched on",
        all_repos,
        actor,
    )
    .await?;
    if change.is_some() {
        resummarize(store, &mut doc).await;
        runs::put_repo(kv, &team, &doc).await?;
    }
    Ok(change)
}

/// "Use recommended" on settings a run kept: each one takes its
/// recommendation, audited and recorded like any change, so its own undo
/// puts it back. Returns the changes made; one that fails stops nothing
/// else and comes back in the second list.
pub async fn use_recommended(
    store: &dyn Store,
    kv: &dyn Kv,
    run_id: &str,
    items: &[(String, String)],
    all_repos: &[String],
    actor: &Actor<'_>,
) -> Result<(Vec<(String, Change)>, Vec<Skipped>), String> {
    let team = store.team().to_string();
    let (run, _) = runs::get_run(kv, &team, run_id)
        .await?
        .ok_or("No such run.")?;
    applied_run(&run)?;
    if run.applied_at.is_none() {
        return Err("Apply the run first; until then, pick Use in Review.".into());
    }
    let mut done = Vec::new();
    let mut skipped = Vec::new();
    let mut repos: Vec<&str> = items.iter().map(|(r, _)| r.as_str()).collect();
    repos.sort_unstable();
    repos.dedup();
    for repo in repos {
        let mut doc = runs::get_repo(kv, &team, run_id, repo)
            .await?
            .ok_or_else(|| format!("{repo} isn't in this run."))?;
        let mut touched = false;
        for (_, sid) in items.iter().filter(|(r, _)| r == repo) {
            let skip = |reason: String| Skipped {
                repo: repo.to_string(),
                setting_id: sid.clone(),
                reason,
            };
            let Some(item) = doc.items.iter().find(|i| &i.setting_id == sid).cloned() else {
                skipped.push(skip("It isn't in this run's plan.".into()));
                continue;
            };
            let Some(e) = registry::get(sid) else {
                skipped.push(skip("Unknown setting.".into()));
                continue;
            };
            // Anything that merges goes on only through Switch on.
            if e.preview_only {
                skipped.push(skip("Use Switch on for a preview.".into()));
                continue;
            }
            match change_one(
                store,
                kv,
                run_id,
                &mut doc,
                e,
                item.recommended.clone(),
                "set to the recommendation",
                all_repos,
                actor,
            )
            .await
            {
                Ok(Some(c)) => {
                    touched = true;
                    done.push((repo.to_string(), c));
                }
                Ok(None) => {}
                Err(m) => skipped.push(skip(m)),
            }
        }
        if touched {
            resummarize(store, &mut doc).await;
            runs::put_repo(kv, &team, &doc).await?;
        }
    }
    Ok((done, skipped))
}

/// Why a run can't be applied now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ApplyRefused {
    NotFound,
    NotReady(RunState),
    Busy,
    Store(String),
}

impl std::fmt::Display for ApplyRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApplyRefused::NotFound => write!(f, "No such run."),
            ApplyRefused::NotReady(s) => write!(
                f,
                "This run can't be applied while it is {}.",
                serde_json::to_value(s)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
                    .unwrap_or_default()
            ),
            ApplyRefused::Busy => write!(f, "This run is being applied already."),
            ApplyRefused::Store(e) => write!(f, "{e}"),
        }
    }
}

/// Applies a run: every repo not done yet, or only `only` (a retry). A repo
/// already done is never applied again.
pub async fn apply_run(
    store: &dyn Store,
    kv: &dyn Kv,
    run_id: &str,
    only: Option<&[String]>,
    all_repos: &[String],
    actor: &Actor<'_>,
) -> Result<(Run, Vec<RepoResult>, Vec<Effect>), ApplyRefused> {
    let team = store.team().to_string();
    let (run, _) = runs::get_run(kv, &team, run_id)
        .await
        .map_err(ApplyRefused::Store)?
        .ok_or(ApplyRefused::NotFound)?;
    let docs = runs::repos_of(kv, &team, &run)
        .await
        .map_err(ApplyRefused::Store)?;
    match runs::state_of(&run, &docs) {
        RunState::Ready | RunState::Done | RunState::Failed => {}
        RunState::Applying => return Err(ApplyRefused::Busy),
        s => return Err(ApplyRefused::NotReady(s)),
    }
    // Take the run: two applies at once can't both start.
    let mut took = false;
    runs::update_run(kv, &team, run_id, |r| {
        if r.state == Some(RunState::Applying) {
            return Err("busy".into());
        }
        r.state = Some(RunState::Applying);
        took = true;
        Ok(())
    })
    .await
    .map_err(|e| {
        if took {
            ApplyRefused::Store(e)
        } else {
            ApplyRefused::Busy
        }
    })?;
    let mut effects = Vec::new();
    for mut doc in docs {
        let wanted = only.is_none_or(|o| o.iter().any(|r| r == &doc.repo));
        let done = doc
            .result
            .as_ref()
            .is_some_and(|r| r.state == ResultState::Done);
        if !wanted || done || doc.state != RepoState::Ready {
            continue;
        }
        let (_, fx) = apply_repo(store, kv, &run, &mut doc, all_repos, actor).await;
        effects.extend(fx);
    }
    let run = settle(kv, &team, run_id, true)
        .await
        .map_err(ApplyRefused::Store)?;
    let results = runs::repos_of(kv, &team, &run)
        .await
        .map_err(ApplyRefused::Store)?
        .into_iter()
        .filter_map(|d| d.result)
        .collect();
    Ok((run, results, effects))
}

/// Settles a run's state from its repos' results: done when every analyzed
/// repo is done, failed otherwise. The team's reviewed settings grow by the
/// run's scope. `force` settles even with repos still unapplied (a guided
/// apply that skipped some).
pub async fn settle(kv: &dyn Kv, team: &str, run_id: &str, force: bool) -> Result<Run, String> {
    let (run, _) = runs::get_run(kv, team, run_id)
        .await?
        .ok_or("No such run.")?;
    let docs = runs::repos_of(kv, team, &run).await?;
    let analyzing =
        docs.iter().any(|d| d.state == RepoState::Analyzing) || docs.len() < run.repos.len();
    let ready: Vec<&RepoDoc> = docs
        .iter()
        .filter(|d| d.state == RepoState::Ready)
        .collect();
    let all_applied = ready.iter().all(|d| d.result.is_some());
    if !force && (analyzing || !all_applied) {
        return Ok(run);
    }
    let ok = !ready.is_empty()
        && ready.iter().all(|d| {
            d.result
                .as_ref()
                .is_some_and(|r| r.state == ResultState::Done)
        })
        && docs.iter().all(|d| d.state != RepoState::Failed);
    let state = if ok { RunState::Done } else { RunState::Failed };
    let any_applied = ready.iter().any(|d| d.result.is_some());
    let run = runs::update_run(kv, team, run_id, |r| {
        r.state = Some(state);
        if any_applied {
            r.applied_at = Some(crate::plugins::now_rfc3339());
        }
        Ok(())
    })
    .await?;
    if any_applied {
        runs::mark_seen(kv, team, &run.scope).await?;
    }
    Ok(run)
}

/// Undoes a run, or only `items` (`(repo, setting_id)`). Each change goes
/// back to its `before` value unless the setting changed since; a row the
/// run created goes away when everything in it is taken back.
pub async fn undo(
    store: &dyn Store,
    kv: &dyn Kv,
    run_id: &str,
    items: Option<&[(String, String)]>,
    all_repos: &[String],
    actor: &Actor<'_>,
) -> Result<UndoReport, String> {
    let team = store.team().to_string();
    let (run, _) = runs::get_run(kv, &team, run_id)
        .await?
        .ok_or("No such run.")?;
    if run.state == Some(RunState::Undone) {
        return Err("This run was undone already.".into());
    }
    if run.state == Some(RunState::Applying) {
        return Err("This run is still being applied.".into());
    }
    let mut report = UndoReport {
        at: crate::plugins::now_rfc3339(),
        partial: items.is_some(),
        ..Default::default()
    };
    let now = crate::plugins::now_rfc3339();
    for mut doc in runs::repos_of(kv, &team, &run).await? {
        let repo = doc.repo.clone();
        let ctx = Ctx {
            repo: &repo,
            all_repos,
            by: actor.email,
            now: now.clone(),
        };
        let mut applied = runs::applied(kv, &team, &repo).await?.unwrap_or_default();
        // Rows the run created whose "never set" values wait for the row to
        // be removed whole.
        let mut pending: Vec<usize> = Vec::new();
        let picked =
            |sid: &str| items.is_none_or(|list| list.iter().any(|(r, s)| *r == repo && s == sid));
        let order: Vec<usize> = (0..doc.changes.len()).rev().collect();
        for i in order {
            let c = doc.changes[i].clone();
            if c.undone || !picked(&c.setting_id) {
                continue;
            }
            let Some(e) = registry::get(&c.setting_id) else {
                continue;
            };
            let skip = |reason: String| Skipped {
                repo: repo.clone(),
                setting_id: c.setting_id.clone(),
                reason,
            };
            if let Some(need) = (actor.missing)(e.capability) {
                report
                    .skipped
                    .push(skip(format!("Needs the \"{need}\" permission")));
                continue;
            }
            let cur = registry::read(store, e, &repo).await?;
            if cur.value != c.after {
                report.skipped.push(skip(format!(
                    "Changed since the run: it is now {}",
                    show(&cur.value)
                )));
                continue;
            }
            let restored = if !c.before.stored && c.row_created {
                pending.push(i);
                Ok(())
            } else if !c.before.stored && registry::unset(store, e, &repo).await? {
                Ok(())
            } else {
                registry::write(store, e, &ctx, &c.before.value)
                    .await
                    .map(|_| ())
                    .map_err(|x| x.to_string())
            };
            if let Err(err) = restored {
                report
                    .skipped
                    .push(skip(format!("Couldn't restore it: {err}")));
                continue;
            }
            doc.changes[i].undone = true;
            applied.values.remove(e.id);
            applied.filled.remove(e.id);
            report.restored += 1;
            record(
                kv,
                &team,
                actor,
                action::SETUP_UNDONE,
                &run.run_id,
                format!(
                    "{repo}: {} put back from {} to {}",
                    e.title,
                    show(&c.after),
                    show(&c.before.value)
                ),
            )
            .await;
        }
        // Remove each created row once every change in it is undone; else
        // write the waiting values back one by one.
        let mut rows: BTreeSet<String> = BTreeSet::new();
        for &i in &pending {
            if let Some(e) = registry::get(&doc.changes[i].setting_id) {
                rows.insert(registry::row(e, &repo));
            }
        }
        for row in rows {
            let in_row: Vec<usize> = (0..doc.changes.len())
                .filter(|&i| {
                    registry::get(&doc.changes[i].setting_id)
                        .is_some_and(|e| registry::row(e, &repo) == row)
                })
                .collect();
            let all_back = in_row.iter().all(|i| doc.changes[*i].undone);
            let waiting: Vec<usize> = in_row
                .iter()
                .copied()
                .filter(|i| pending.contains(i))
                .collect();
            if all_back {
                if let Some(e) = registry::get(&doc.changes[waiting[0]].setting_id) {
                    registry::remove_row(store, e, &repo).await?;
                    for o in registry::same_row(e) {
                        applied.filled.remove(o.id);
                    }
                }
            } else {
                for i in waiting {
                    let c = &doc.changes[i];
                    if let Some(e) = registry::get(&c.setting_id) {
                        registry::write(store, e, &ctx, &c.before.value)
                            .await
                            .map_err(|x| x.to_string())?;
                    }
                }
            }
        }
        runs::put_applied(kv, &team, &repo, &applied).await?;
        runs::put_repo(kv, &team, &doc).await?;
    }
    let whole = items.is_none();
    let report2 = report.clone();
    runs::update_run(kv, &team, run_id, |r| {
        if whole {
            r.state = Some(RunState::Undone);
        }
        r.undo = Some(report2.clone());
        Ok(())
    })
    .await?;
    Ok(report)
}

#[cfg(test)]
mod tests;
