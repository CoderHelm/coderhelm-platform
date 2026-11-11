//! Setup's analysis of one repo (`setup_analyze`), and automatic setup of
//! newly connected repos (`common::setup` has the rules).
//!
//! Per repo: read the facts from GitHub (bounded: the tree, a few known
//! files, the workflows, labels, branch protection, tags, the last merged
//! pull requests and the open ones), run the detectors, make the one AI
//! read (counted against the team's token limits, on the light model,
//! skipped when the team is over its limit), and save the plan. A run in
//! auto mode, or one started by itself for a new repo, applies the plan as
//! soon as it is made.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use common::plugins::audit::ActorKind;
use common::plugins::kv::{Dynamo as DynamoKv, Expect};
use common::setup::apply::{self, Actor, BudgetGate};
use common::setup::facts::{self, PrDigest, RepoFacts};
use common::setup::plan::ItemDecision;
use common::setup::registry::{self, Effect};
use common::setup::runs::{self, Mode, RepoDoc, RepoState, Run, RunState};
use common::setup::store::DynamoStore;
use common::setup::{ai, ESTIMATE_TOKENS_PER_REPO};
use serde_json::Value;
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::models::{SetupAnalyzeMessage, TokenUsage, WorkerMessage};
use crate::WorkerState;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Merged pull requests whose files are read.
const FILES_FOR: usize = 30;
/// Merged pull requests whose review comments are read.
const COMMENTS_FOR: usize = 20;
/// Paths kept from the tree.
const MAX_PATHS: usize = 5_000;

fn kv(state: &WorkerState) -> DynamoKv {
    DynamoKv {
        client: state.dynamo.clone(),
        table: state.config.settings_table_name.clone(),
    }
}

fn store(state: &WorkerState, team: &str) -> DynamoStore {
    DynamoStore {
        client: state.dynamo.clone(),
        settings_table: state.config.settings_table_name.clone(),
        e2e_table: state.config.tests_table_name.clone(),
        team: team.to_string(),
    }
}

fn time(v: &Value) -> Option<DateTime<Utc>> {
    v.as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))
}

fn by_person(user: &Value) -> bool {
    user["type"].as_str() != Some("Bot") && !user["login"].as_str().unwrap_or("").ends_with("[bot]")
}

/// Reads one repo's facts. Every read past the repo itself is best effort:
/// what can't be read is simply missing from the facts.
async fn gather(gh: &GitHubClient, repo: &str) -> Result<RepoFacts, Error> {
    let (owner, name) = repo.split_once('/').ok_or("bad repo name")?;
    let info = gh.get_api(&format!("/repos/{owner}/{name}")).await?;
    let branch = info["default_branch"]
        .as_str()
        .unwrap_or("main")
        .to_string();
    let mut f = RepoFacts {
        repo: repo.to_string(),
        html_url: info["html_url"]
            .as_str()
            .map(String::from)
            .unwrap_or_else(|| format!("https://github.com/{repo}")),
        allow_squash: info["allow_squash_merge"].as_bool(),
        allow_merge: info["allow_merge_commit"].as_bool(),
        allow_rebase: info["allow_rebase_merge"].as_bool(),
        default_branch: branch.clone(),
        ..Default::default()
    };
    if let Ok(tree) = gh.get_tree(owner, name, &branch).await {
        f.paths = tree
            .into_iter()
            .filter(|t| t.entry_type == "blob")
            .map(|t| t.path)
            .take(MAX_PATHS)
            .collect();
    }
    for p in facts::READ_FILES {
        if !f.paths.iter().any(|x| x == p) {
            continue;
        }
        if let Ok(Some(text)) = gh.read_file_if_exists(owner, name, p, &branch).await {
            f.files.insert(
                p.to_string(),
                common::truncate_str(&text, facts::MAX_FILE_CHARS).to_string(),
            );
        }
    }
    let workflows: Vec<String> = f
        .paths
        .iter()
        .filter(|p| {
            p.starts_with(".github/workflows/") && (p.ends_with(".yml") || p.ends_with(".yaml"))
        })
        .take(facts::MAX_WORKFLOWS)
        .cloned()
        .collect();
    for p in workflows {
        if let Ok(Some(text)) = gh.read_file_if_exists(owner, name, &p, &branch).await {
            f.workflows.push((p, text));
        }
    }
    f.labels = gh
        .list_repo_labels(owner, name)
        .await
        .map(|l| l.into_iter().map(|(n, _)| n).collect())
        .unwrap_or_default();
    // Required checks from rules and classic protection; an app without
    // administration read sees no protection, which counts as unknown.
    let rules = gh.branch_rules(owner, name, &branch).await.ok();
    let protection = gh.required_status_checks(owner, name, &branch).await.ok();
    f.protection_read = rules.is_some() || protection.is_some();
    f.required_checks = common::e2e::check_gate::required(rules.as_ref(), protection.as_ref());
    if let Ok(tags) = gh.list_tags(owner, name).await {
        f.tags = tags
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| t["name"].as_str().map(String::from))
            .take(30)
            .collect();
    }
    // The last merged pull requests.
    let closed = gh
        .get_api(&format!(
            "/repos/{owner}/{name}/pulls?state=closed&sort=updated&direction=desc&per_page=100"
        ))
        .await
        .unwrap_or(Value::Null);
    for pr in closed.as_array().into_iter().flatten() {
        if f.merged.len() >= facts::MAX_MERGED {
            break;
        }
        let Some(merged_at) = time(&pr["merged_at"]) else {
            continue;
        };
        let number = pr["number"].as_u64().unwrap_or(0);
        let mut d = PrDigest {
            number,
            title: pr["title"].as_str().unwrap_or("").to_string(),
            url: pr["html_url"].as_str().unwrap_or("").to_string(),
            author: pr["user"]["login"].as_str().unwrap_or("").to_string(),
            labels: pr["labels"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|l| l["name"].as_str().map(String::from))
                .collect(),
            hours_to_merge: time(&pr["created_at"])
                .map(|c| (merged_at - c).num_minutes() as f64 / 60.0),
            merged_at: Some(merged_at.to_rfc3339()),
            ..Default::default()
        };
        let i = f.merged.len();
        if i < FILES_FOR {
            if let Ok(files) = gh.list_pull_files(owner, name, number).await {
                d.files = files.into_iter().map(|(p, _)| p).collect();
                d.files_read = true;
            }
        }
        if i < COMMENTS_FOR {
            if let Ok(comments) = gh.get_review_comments(owner, name, number).await {
                d.review_comments = comments
                    .iter()
                    .filter(|c| by_person(&c["user"]))
                    .filter_map(|c| c["body"].as_str())
                    .map(|b| common::truncate_str(b, 300).to_string())
                    .take(8)
                    .collect();
            }
        }
        f.merged.push(d);
    }
    // Open pull requests and, for those old enough to matter, their
    // timelines: what the stale cleanup would have done.
    let open = gh
        .list_open_pulls_all(owner, name, 3)
        .await
        .unwrap_or_default();
    let now = Utc::now();
    let window = chrono::Duration::days(facts::STALE_WINDOW_DAYS + 3);
    for pr in open {
        let old = time(&pr["created_at"]).is_some_and(|c| now - c >= window);
        if !old {
            f.open.push((pr, vec![]));
            continue;
        }
        if f.open.iter().filter(|(_, t)| !t.is_empty()).count() >= facts::MAX_STALE_READS {
            f.open_not_read += 1;
            continue;
        }
        let number = pr["number"].as_u64().unwrap_or(0);
        let timeline: Vec<Value> = gh
            .list_issue_timeline(owner, name, number)
            .await
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default();
        // A pull request with an empty timeline still counts from opening.
        f.open.push((pr, timeline));
    }
    Ok(f)
}

/// The AI read: the checked recommendations and the tokens it used.
async fn ai_read(
    state: &WorkerState,
    team: &str,
    f: &RepoFacts,
    current: &HashMap<&'static str, registry::Current>,
) -> Result<(HashMap<&'static str, ai::Inferred>, u64), Error> {
    let provider = crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        team,
    )
    .await?;
    let mut usage = TokenUsage::default();
    let prompt = ai::prompt(f, current);
    // Counted under the message's usage feature (onboarding).
    let answer = crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        ai::SYSTEM,
        &prompt,
        &ai::output_schema(),
        true,
        "low",
        ai::MAX_OUTPUT_TOKENS,
        &mut usage,
    )
    .await?;
    let used = usage.input_tokens + usage.output_tokens;
    Ok((ai::parse(&answer, f), used))
}

/// `setup_analyze`: one repo of a run.
pub async fn analyze(state: &WorkerState, msg: &SetupAnalyzeMessage) {
    let team = msg.team_id.as_str();
    let kv = kv(state);
    let Ok(Some((run, _))) = runs::get_run(&kv, team, &msg.run_id).await else {
        warn!(team, run = %msg.run_id, "Setup: no such run");
        return;
    };
    let doc = runs::get_repo(&kv, team, &run.run_id, &msg.repo)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| RepoDoc::new(&run.run_id, &msg.repo));
    if doc.state != RepoState::Analyzing {
        return; // already analyzed (a redelivered message)
    }
    if let Err(e) = analyze_repo(state, team, &run, doc).await {
        warn!(team, run = %run.run_id, repo = %msg.repo, error = %e, "Setup: analysis failed");
        apply::fail_analysis(
            &kv,
            team,
            &run.run_id,
            &msg.repo,
            &format!("Couldn't read the repo: {e}"),
        )
        .await;
    }
    if run.mode == Mode::Auto || run.automatic {
        if let Err(e) = apply::settle(&kv, team, &run.run_id, false).await {
            warn!(team, run = %run.run_id, error = %e, "Setup: could not settle the run");
        }
    }
}

async fn analyze_repo(
    state: &WorkerState,
    team: &str,
    run: &Run,
    mut doc: RepoDoc,
) -> Result<(), Error> {
    let team = team.to_string();
    let kv = kv(state);
    let st = store(state, &team);
    doc.progress = Some("Reading the repo".into());
    runs::put_repo(&kv, &team, &doc).await?;
    let Some(iid) = super::review_reminder::team_installation_id(state, &team).await else {
        apply::fail_analysis(
            &kv,
            &team,
            &run.run_id,
            &doc.repo,
            "The GitHub App isn't installed for this team.",
        )
        .await;
        return Ok(());
    };
    let gh = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        iid,
        &state.http,
    )?;
    let f = gather(&gh, &doc.repo).await?;
    let ids: Vec<&str> = run.scope.iter().map(String::as_str).collect();
    let over = super::check_budget_exceeded(state, &team).await;
    let mode = if run.automatic { Mode::Auto } else { run.mode };
    let (inferred, used, warning) = match apply::budget_gate(mode, over.is_some(), ai::needed(&ids)) {
        BudgetGate::Stop => {
            apply::fail_analysis(
                &kv,
                &team,
                &run.run_id,
                &doc.repo,
                &format!(
                    "Nothing was applied: {}",
                    over.unwrap_or_else(|| "the team's token limit is reached".into())
                ),
            )
            .await;
            return Ok(());
        }
        BudgetGate::SkipAi => (
            HashMap::new(),
            0,
            Some(format!(
                "The AI read was skipped ({}); recommendations come from what was detected and CoderHelm's defaults.",
                over.unwrap_or_default()
            )),
        ),
        BudgetGate::Proceed if ai::needed(&ids) => {
            doc.progress = Some("Reading pull requests and reviews".into());
            runs::put_repo(&kv, &team, &doc).await?;
            let current = registry::read_all(&st, &doc.repo).await?;
            match ai_read(state, &team, &f, &current).await {
                Ok((i, used)) => (i, used, None),
                Err(e) => {
                    warn!(repo = %doc.repo, error = %e, "Setup: the AI read failed");
                    (
                        HashMap::new(),
                        0,
                        Some("The AI read didn't finish; recommendations come from what was detected and CoderHelm's defaults.".into()),
                    )
                }
            }
        }
        BudgetGate::Proceed => (HashMap::new(), 0, None),
    };
    let mut doc =
        apply::finish_analysis(&st, &kv, run, &f, &inferred, used, warning, Utc::now()).await?;
    info!(team, run = %run.run_id, repo = %doc.repo, items = doc.items.len(), tokens = used, "Setup: repo analyzed");
    if mode == Mode::Auto {
        let all = super::infra_repo::team_repos(state, &team)
            .await
            .into_iter()
            .map(|r| r.full_name)
            .collect::<Vec<_>>();
        let lacks = run.lacks.clone();
        let missing = move |cap: &str| lacks.get(cap).cloned();
        let actor = Actor {
            email: &run.by,
            kind: if run.automatic {
                ActorKind::System
            } else {
                ActorKind::Person
            },
            missing: &missing,
        };
        let (res, effects) = apply::apply_repo(&st, &kv, run, &mut doc, &all, &actor).await;
        for Effect::BuildGraph { repo } in &effects {
            if common::code_graph::is_enabled(
                &state.dynamo,
                &state.config.settings_table_name,
                &team,
                repo,
            )
            .await
            {
                super::code_graph::queue_full_index(state, &team, repo, iid, false).await;
            }
        }
        info!(team, run = %run.run_id, repo = %doc.repo, changed = res.changed, summary = %res.summary.text, "Setup: applied by itself");
    }
    Ok(())
}

/// After repos are connected: when the team turned on
/// `auto_setup_new_repos`, the repos never set up get the last reviewed
/// choices, in a run marked automatic. Only settings the team has reviewed
/// are in it: nothing new turns on by itself.
pub async fn after_onboard(state: &WorkerState, team: &str, repos: &[String]) {
    let kv = kv(state);
    let Ok(settings) = runs::settings(&kv, team).await else {
        return;
    };
    if !settings.auto_setup_new_repos || repos.is_empty() {
        return;
    }
    let Ok(Some(seen)) = runs::seen(&kv, team).await else {
        return;
    };
    let history = runs::list(&kv, team, 50).await.unwrap_or_default();
    let Some(last) = history
        .iter()
        .find(|r| !r.automatic && r.state == Some(RunState::Done))
    else {
        info!(
            team,
            "Setup: no reviewed run yet, so new repos wait to be set up"
        );
        return;
    };
    let mut fresh = Vec::new();
    for r in repos {
        if matches!(runs::applied(&kv, team, r).await, Ok(None)) {
            fresh.push(r.clone());
        }
    }
    if fresh.is_empty() {
        return;
    }
    let scope: Vec<String> = registry::REGISTRY
        .iter()
        .filter(|e| seen.ids.contains(e.id) && last.scope.iter().any(|s| s == e.id))
        .map(|e| e.id.to_string())
        .collect();
    let ids: Vec<&str> = scope.iter().map(String::as_str).collect();
    let estimate = if ai::needed(&ids) {
        fresh.len() as u64 * ESTIMATE_TOKENS_PER_REPO
    } else {
        0
    };
    let mut run = Run::new(
        "automatic",
        Mode::Auto,
        last.choice,
        false,
        fresh.clone(),
        scope,
        estimate,
    );
    run.automatic = true;
    run.lacks = last.lacks.clone();
    run.decisions = reviewed_decisions(&last.decisions, &fresh);
    if runs::put_run(&kv, team, &run, Expect::Absent)
        .await
        .is_err()
    {
        return;
    }
    for r in &fresh {
        let _ = runs::put_repo(&kv, team, &RepoDoc::new(&run.run_id, r)).await;
        let msg = WorkerMessage::SetupAnalyze(SetupAnalyzeMessage {
            team_id: team.to_string(),
            run_id: run.run_id.clone(),
            repo: r.clone(),
        });
        let Ok(body) = serde_json::to_string(&msg) else {
            continue;
        };
        if let Err(e) = state
            .sqs
            .send_message()
            .queue_url(&state.config.ticket_queue_url)
            .message_body(body)
            .send()
            .await
        {
            warn!(team, repo = %r, error = %e, "Setup: could not queue an automatic analysis");
            apply::fail_analysis(
                &kv,
                team,
                &run.run_id,
                r,
                "The analysis couldn't be started.",
            )
            .await;
        }
    }
    info!(team, run = %run.run_id, repos = fresh.len(), "Setup: new repos set up automatically");
}

/// The picks of the last reviewed run, carried to new repos: a setting's
/// pick when every repo of that run got the same one (an edited value too).
pub fn reviewed_decisions(last: &[ItemDecision], repos: &[String]) -> Vec<ItemDecision> {
    let mut by_setting: BTreeMap<&str, Vec<&ItemDecision>> = BTreeMap::new();
    for d in last {
        by_setting.entry(d.setting_id.as_str()).or_default().push(d);
    }
    let mut out = Vec::new();
    for (sid, picks) in by_setting {
        let first = picks[0];
        let same = picks
            .iter()
            .all(|p| p.decision == first.decision && p.value == first.value);
        if !same {
            continue;
        }
        for r in repos {
            out.push(ItemDecision {
                repo: r.clone(),
                setting_id: sid.to_string(),
                decision: first.decision,
                value: first.value.clone(),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::setup::plan::Decision;
    use serde_json::json;

    #[test]
    fn new_repos_get_the_picks_every_reviewed_repo_shared() {
        let d = |repo: &str, sid: &str, decision: Decision, value: Option<Value>| ItemDecision {
            repo: repo.into(),
            setting_id: sid.into(),
            decision,
            value,
        };
        let last = vec![
            d("a/1", "reviewer.label", Decision::Keep, None),
            d("a/2", "reviewer.label", Decision::Keep, None),
            d(
                "a/1",
                "stale_prs.stale_days",
                Decision::Edit,
                Some(json!(30)),
            ),
            d(
                "a/2",
                "stale_prs.stale_days",
                Decision::Edit,
                Some(json!(14)),
            ),
        ];
        let out = reviewed_decisions(&last, &["n/1".to_string()]);
        assert_eq!(out, vec![d("n/1", "reviewer.label", Decision::Keep, None)]);
    }
}
