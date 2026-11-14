//! Security fixes: the worker's side (`common::security_fixes` has the rules).
//!
//! It rides on the review-reminder tick (EventBridge, every 20 minutes) and
//! on `security_event` messages from the gateway (a Dependabot alert, a
//! GitHub advisory, a push to a default branch, a person's "check now").
//! For each team that turned it on:
//!
//! - **Inventory**, once a day per repository and on every push that touches
//!   a dependency file: GitHub's dependency graph SBOM, the scopes npm's
//!   `package-lock.json` and `poetry.lock` record, and Dockerfile `FROM`
//!   lines (`DEPS#<repo>`).
//! - **Sources**: Dependabot alerts (webhook, and a daily read of the open
//!   ones when the app may read them), GitHub advisories matched against the
//!   dependency lists, OSV's API (`querybatch`, then each record), the CISA
//!   list of exploited vulnerabilities and FIRST's EPSS scores, daily. There
//!   is no local OSV mirror: OSV is read through its API ("api" mode).
//! - **Triage**: dev-only dependencies are not loaded (no agent). Otherwise
//!   the package and the advisory's named functions are searched for in the
//!   code, and one short model call reads the hits and gives a verdict with
//!   file and line, bounded per finding and per team per day. govulncheck
//!   isn't run (the worker has no Go toolchain); Go findings use the same
//!   symbol search.
//! - **Fix runs**: P0 and P1 at once, P2 daily and P3 weekly, grouped per
//!   repository and ecosystem, through the normal run pipeline (`SEC-`
//!   tickets). An open Dependabot or Renovate pull request for the package is
//!   adopted instead. Nothing opens or posts in a dry run.
//! - **Release**: a finding's `fixing` and `pr_open` follow GitHub and the
//!   run's record (`release`): a pull request closed unmerged or a run that
//!   ended without one gives its findings back, on the webhook, on the
//!   person's cancel, and on every sweep before anything else, on a clock
//!   of its own.
//! - **Cards**: P0 and P1 post to the team's alert channel once, then on the
//!   pull request opening, CI passing and the merge, and at half the
//!   deadline and the deadline. Every card is claimed once.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{Duration, Utc};
use common::security_fixes::{
    self as sec, DailySlots, Dep, Evidence, EvidenceLine, Finding, FindingState, FixUnit,
    Observation, Scope, SecurityConfig, Stage, Status, Verdict,
};
use serde_json::{json, Value};
use tracing::{info, warn};

use common::problems::{self, Check, Kind, Outcome};

use crate::clients::github::GitHubClient;
use crate::models::{SecurityEventMessage, TicketMessage, TicketSource, TokenUsage, WorkerMessage};
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;

/// Releasing a finding from a pull request or a run that is over.
#[path = "security_release.rs"]
pub(crate) mod release;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Item = HashMap<String, AttributeValue>;

/// Time one tick may spend on security work.
const TICK_BUDGET_SECS: u64 = 300;
/// Time past that budget the sweep may still spend releasing findings for
/// the teams the budget never reached (`release`).
const RELEASE_BUDGET_SECS: u64 = 240;
/// Model calls triage makes per team per tick at most (one call reads one
/// package at one version, with every advisory it has); fewer when the
/// day's token cap is near (`reachability::model_calls_allowed`). With a
/// tick every 20 minutes, 50 reads take five ticks under the 400k cap.
const TRIAGE_PER_TICK: usize = 12;
/// Groups read per tick. The code check reads the repository once per tick
/// (one tarball) and settles most groups without the model, so every open
/// group fits in one tick.
const TRIAGE_SEARCH_PER_TICK: usize = 600;
/// Characters of evidence one triage question carries.
const TRIAGE_MAX_CHARS: usize = 8_000;
/// Days before a verdict is read again (the code changes; an unchanged
/// read reuses the verdict without the model).
const TRIAGE_RECHECK_DAYS: i64 = 7;
/// What the triage key starts with: a new way of reading the code reads
/// every finding again once.
const TRIAGE_VERSION: &str = "code-2";
/// OSV records read per sync (shared by every repository in the tick).
const OSV_HYDRATE_MAX: usize = 1_500;
/// OSV records read at once.
const OSV_PARALLEL: usize = 8;
/// Bytes of dependency list kept per repository (DynamoDB items stop at
/// 400 KB). Matching always uses the whole list.
const MAX_DEPS_BYTES: usize = 340_000;
/// Findings read per team.
const MAX_FINDINGS: usize = 5_000;
/// Dependencies kept per repository.
const MAX_DEPS: usize = 3_000;

// ── Storage ─────────────────────────────────────────────────────────────

fn s(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

pub async fn load_config(state: &WorkerState, team_id: &str) -> Option<SecurityConfig> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sec::CONFIG_SK))
        .send()
        .await
        .ok()?;
    out.item()
        .and_then(|i| SecurityConfig::from_json(&s(i, "config")))
}

pub async fn load_findings(state: &WorkerState, team_id: &str) -> Vec<(Finding, FindingState)> {
    let mut out = vec![];
    let mut start: Option<Item> = None;
    loop {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
            .expression_attribute_values(":p", attr_s(team_id))
            .expression_attribute_values(":pfx", attr_s(sec::FINDING_PREFIX))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(resp) = resp else {
            warn!(team_id, "security: findings could not be read");
            break;
        };
        for i in resp.items() {
            let Ok(f) = serde_json::from_str::<Finding>(&s(i, "finding")) else {
                continue;
            };
            let st = serde_json::from_str::<FindingState>(&s(i, "state")).unwrap_or_default();
            out.push((f, st));
        }
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() && out.len() < MAX_FINDINGS => start = Some(k.clone()),
            _ => break,
        }
    }
    out
}

async fn save_finding(state: &WorkerState, team_id: &str, f: &Finding, st: &FindingState) {
    let (Ok(fj), Ok(sj)) = (serde_json::to_string(f), serde_json::to_string(st)) else {
        return;
    };
    let r = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&sec::finding_sk(&f.id)))
        .item("finding", attr_s(&fj))
        .item("state", attr_s(&sj))
        .item("status", attr_s(f.status.as_str()))
        .item("tier", attr_s(f.tier.map(|t| t.as_str()).unwrap_or("")))
        .item("repo", attr_s(&f.repo))
        .item("updated_at", attr_s(&f.updated_at))
        .send()
        .await;
    if let Err(e) = r {
        warn!(team_id, finding = %f.id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "security: could not save a finding");
    }
}

/// One timeline line for a finding (kept 180 days).
pub async fn add_event(
    state: &WorkerState,
    team_id: &str,
    finding_id: &str,
    text: &str,
    link: Option<&str>,
) {
    let now = Utc::now();
    let n = now.timestamp_subsec_micros() % 1000;
    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&sec::event_sk(finding_id, &now, n)))
        .item("at", attr_s(&now.to_rfc3339()))
        .item("text", attr_s(common::truncate_str(text, 500)))
        .item(
            "ttl",
            attr_n(now.timestamp() + sec::EVENT_TTL_DAYS * 86_400),
        );
    if let Some(l) = link {
        put = put.item("link", attr_s(l));
    }
    if let Err(e) = put.send().await {
        warn!(team_id, finding_id, error = %e, "security: could not record a timeline line");
    }
}

async fn load_deps(state: &WorkerState, team_id: &str, repo: &str) -> Option<(Vec<Dep>, String)> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sec::deps_sk(repo)))
        .send()
        .await
        .ok()?;
    let i = out.item()?;
    let deps = serde_json::from_str::<Vec<Dep>>(&s(i, "deps")).ok()?;
    Some((deps, s(i, "updated_at")))
}

/// The dependency list kept for a repository: whole, or cut to fit an item.
fn deps_to_keep(deps: &[Dep]) -> (String, usize) {
    let mut n = deps.len().min(MAX_DEPS);
    loop {
        let text = serde_json::to_string(&deps[..n]).unwrap_or_else(|_| "[]".into());
        if text.len() <= MAX_DEPS_BYTES || n == 0 {
            return (text, n);
        }
        n = n * 9 / 10;
    }
}

async fn save_deps(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    deps: &[Dep],
    status: &sec::InventoryStatus,
    unpinned: usize,
    branch: &str,
) {
    let (text, kept) = deps_to_keep(deps);
    if kept < deps.len() {
        warn!(
            team_id,
            repo,
            total = deps.len(),
            kept,
            "security: the dependency list was cut to fit one record"
        );
    }
    let ecos = serde_json::to_string(&sec::ecosystems(deps)).unwrap_or_default();
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sec::deps_sk(repo)))
        .update_expression(
            "SET #repo = :r, deps = :d, #count = :c, kept = :k, ecosystems = :e, unpinned = :u, \
             inventory_state = :s, inventory_reason = :why, updated_at = :t, read_at = :t, \
             branch = :b",
        )
        .expression_attribute_names("#repo", "repo")
        .expression_attribute_names("#count", "count")
        .expression_attribute_values(":r", attr_s(repo))
        .expression_attribute_values(":d", attr_s(&text))
        .expression_attribute_values(":c", attr_n(deps.len()))
        .expression_attribute_values(":k", attr_n(kept))
        .expression_attribute_values(":e", attr_s(&ecos))
        .expression_attribute_values(":u", attr_n(unpinned))
        .expression_attribute_values(":s", attr_s(&status.state))
        .expression_attribute_values(":why", attr_s(&status.reason))
        .expression_attribute_values(":b", attr_s(branch))
        .expression_attribute_values(":t", attr_s(&Utc::now().to_rfc3339()))
        .send()
        .await
    {
        warn!(team_id, repo, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "security: could not save the dependency list");
    }
}

/// Sets fields of a repository's dependency record (its read state, its
/// Dependabot access) without touching the list.
async fn set_repo_fields(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    fields: &[(&str, AttributeValue)],
) {
    let mut expr = vec!["#repo = :repo".to_string()];
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sec::deps_sk(repo)))
        .expression_attribute_names("#repo", "repo")
        .expression_attribute_values(":repo", attr_s(repo));
    for (i, (k, v)) in fields.iter().enumerate() {
        expr.push(format!("#k{i} = :v{i}"));
        req = req
            .expression_attribute_names(format!("#k{i}"), *k)
            .expression_attribute_values(format!(":v{i}"), v.clone());
    }
    if let Err(e) = req
        .update_expression(format!("SET {}", expr.join(", ")))
        .send()
        .await
    {
        warn!(team_id, repo, error = %e, "security: could not save the repository's state");
    }
}

/// Sets fields of the team's source status record.
async fn set_team_state(state: &WorkerState, team_id: &str, fields: &[(&str, AttributeValue)]) {
    if fields.is_empty() {
        return;
    }
    let mut expr = vec![];
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sec::STATE_SK));
    for (i, (k, v)) in fields.iter().enumerate() {
        expr.push(format!("#k{i} = :v{i}"));
        req = req
            .expression_attribute_names(format!("#k{i}"), *k)
            .expression_attribute_values(format!(":v{i}"), v.clone());
    }
    if let Err(e) = req
        .update_expression(format!("SET {}", expr.join(", ")))
        .send()
        .await
    {
        warn!(team_id, error = %e, "security: could not save the source status");
    }
}

/// Records one run of a source's check on `sk` (a repository's dependency
/// row, or the team's source status): a pass clears what that check set.
async fn record_problem(
    state: &WorkerState,
    team_id: &str,
    sk: &str,
    check: Check,
    outcome: Outcome,
) {
    problems::record_on(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        sk,
        check,
        outcome,
        &Utc::now().to_rfc3339(),
    )
    .await;
}

async fn claim(state: &WorkerState, pk: &str, sk: &str, ttl: u64) -> bool {
    common::claim::claim(
        &state.dynamo,
        &state.config.settings_table_name,
        pk,
        sk,
        ttl,
    )
    .await
    .won_or_failed_open()
}

async fn release(state: &WorkerState, pk: &str, sk: &str) {
    common::claim::release(&state.dynamo, &state.config.settings_table_name, pk, sk).await;
}

/// The repositories in scope: the team's enabled repositories, narrowed to
/// the settings' list.
async fn team_repos(state: &WorkerState, team_id: &str, cfg: &SecurityConfig) -> Vec<String> {
    let mut out = vec![];
    let mut start: Option<Item> = None;
    for _ in 0..20 {
        let Ok(resp) = state
            .dynamo
            .query()
            .table_name(&state.config.repos_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(team_id))
            .expression_attribute_values(":prefix", attr_s("REPO#"))
            .set_exclusive_start_key(start.take())
            .send()
            .await
        else {
            break;
        };
        for i in resp.items() {
            let enabled = i
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            let name = s(i, "repo_name");
            if enabled && sec::valid_repo(&name) && cfg.repos.includes(&name) {
                out.push(name);
            }
        }
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out.sort();
    out.dedup();
    out
}

fn github(state: &WorkerState, installation_id: u64) -> Result<GitHubClient, Error> {
    GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        installation_id,
        &state.http,
    )
}

// ── Inventory ───────────────────────────────────────────────────────────

/// Reads a repository's dependencies now and saves them.
async fn refresh_inventory(
    state: &WorkerState,
    team_id: &str,
    gh: &GitHubClient,
    repo: &str,
) -> Result<Vec<Dep>, Error> {
    let (owner, name) = repo.split_once('/').ok_or("bad repo")?;
    let mut deps: Vec<Dep> = vec![];
    let mut unpinned = 0;
    let status = match gh
        .get_status(&format!("/repos/{owner}/{name}/dependency-graph/sbom"))
        .await
    {
        Ok((code, sbom)) if code < 300 => {
            (deps, unpinned) = sec::inventory_from_sbom(&sbom);
            sec::InventoryStatus {
                state: "ok".into(),
                reason: String::new(),
            }
        }
        Ok((code, body)) => {
            let s = sec::sbom_failure(code, &body);
            info!(team_id, repo, code, reason = %s.reason, "security: no dependency graph SBOM");
            s
        }
        Err(e) => {
            warn!(team_id, repo, error = %e, "security: the dependency graph SBOM could not be read");
            sec::InventoryStatus {
                state: "error".into(),
                reason: "GitHub didn't answer for the dependency list; it is tried again on the next check.".into(),
            }
        }
    };
    let branch = gh.get_default_branch(owner, name).await?;
    let tree = gh
        .get_status(&format!(
            "/repos/{owner}/{name}/git/trees/{}?recursive=1",
            urlencoding::encode(&branch)
        ))
        .await
        .ok()
        .filter(|(c, _)| *c < 300)
        .map(|(_, t)| t)
        .unwrap_or(Value::Null);
    let paths: Vec<String> = tree["tree"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|e| e["type"].as_str() == Some("blob"))
        .filter_map(|e| e["path"].as_str().map(String::from))
        .filter(|p| !p.contains("node_modules/") && !p.contains("vendor/"))
        .collect();
    // Scopes and manifests from the lockfiles that record them.
    let mut npm_locks: Vec<(String, HashMap<String, Scope>)> = vec![];
    for p in paths
        .iter()
        .filter(|p| p.ends_with("package-lock.json"))
        .take(5)
    {
        let Ok(text) = gh.read_file(owner, name, p, &branch).await else {
            continue;
        };
        let Ok(lock) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        // Each version to the lockfile that installs it, with its path.
        sec::place_npm_copies(&mut deps, p, &sec::npm_lock_copies(&lock));
        npm_locks.push((p.clone(), sec::npm_lock_scopes(&lock)));
    }
    // A version no lockfile names exactly: its scope (and lockfile) by name,
    // as before.
    for (p, scopes) in &npm_locks {
        let mut unplaced: Vec<Dep> = deps
            .iter()
            .filter(|d| d.ecosystem == "npm" && d.manifest == sec::SBOM_MANIFEST)
            .cloned()
            .collect();
        apply_scopes(&mut unplaced, "npm", p, scopes);
        for u in unplaced {
            if let Some(d) = deps.iter_mut().find(|d| {
                d.ecosystem == "npm"
                    && d.manifest == sec::SBOM_MANIFEST
                    && d.name == u.name
                    && d.version == u.version
            }) {
                *d = u;
            }
        }
    }
    for p in paths.iter().filter(|p| p.ends_with("poetry.lock")).take(5) {
        if let Ok(text) = gh.read_file(owner, name, p, &branch).await {
            apply_scopes(&mut deps, "PyPI", p, &sec::poetry_lock_scopes(&text));
        }
    }
    for p in paths.iter().filter(|p| sec::is_dockerfile(p)).take(10) {
        if let Ok(text) = gh.read_file(owner, name, p, &branch).await {
            for (image, tag) in sec::dockerfile_images(&text) {
                let d = Dep {
                    ecosystem: "docker".into(),
                    name: image,
                    version: tag,
                    manifest: p.clone(),
                    scope: Scope::Runtime,
                    path: vec![],
                };
                if !deps.contains(&d) {
                    deps.push(d);
                }
            }
        }
    }
    save_deps(state, team_id, repo, &deps, &status, unpinned, &branch).await;
    Ok(deps)
}

fn apply_scopes(
    deps: &mut [Dep],
    ecosystem: &str,
    manifest: &str,
    scopes: &HashMap<String, Scope>,
) {
    for d in deps.iter_mut().filter(|d| d.ecosystem == ecosystem) {
        let key = if ecosystem == "PyPI" {
            d.name.to_ascii_lowercase()
        } else {
            d.name.clone()
        };
        if let Some(sc) = scopes.get(&key) {
            d.scope = *sc;
            if d.manifest == sec::SBOM_MANIFEST {
                d.manifest = manifest.to_string();
            }
        }
    }
}

// ── Observations into findings ─────────────────────────────────────────

/// What applying observations changed, for the caller's follow-up.
#[derive(Default)]
struct Applied {
    created: usize,
    changed: Vec<String>,
}

async fn upsert(
    state: &WorkerState,
    team_id: &str,
    cfg: &SecurityConfig,
    findings: &mut Vec<(Finding, FindingState)>,
    observations: Vec<Observation>,
) -> Applied {
    let now = Utc::now();
    let mut out = Applied::default();
    for o in observations {
        if o.package.is_empty() || o.advisory.ids.is_empty() {
            continue;
        }
        let idx = findings.iter().position(|(f, _)| {
            sec::merges_into_at(
                f,
                &o.advisory.ids,
                &o.repo,
                &o.manifest,
                &o.package,
                &o.version,
            )
        });
        let existing = idx.map(|i| findings[i].clone());
        let before = existing.as_ref().map(|(f, _)| (f.status, f.tier));
        let (mut f, st, created) = sec::apply(existing.clone(), &o, now);
        sec::retier(&mut f, &st, cfg);
        // Seen again with nothing new: no write.
        let unchanged = existing.as_ref().is_some_and(|(old_f, old_st)| {
            let mut same = f.clone();
            same.updated_at = old_f.updated_at.clone();
            same == *old_f && st == *old_st
        });
        if unchanged {
            continue;
        }
        save_finding(state, team_id, &f, &st).await;
        if created {
            out.created += 1;
            add_event(
                state,
                team_id,
                &f.id,
                &format!(
                    "Seen by {}: {} in {} {} ({} ids, one finding)",
                    source_name(&o.advisory.source),
                    f.advisory.primary,
                    f.package,
                    f.version,
                    f.advisory.ids.len()
                ),
                o.alert.as_ref().map(|a| a.url.as_str()),
            )
            .await;
        } else if let Some((status, tier)) = before {
            if status != f.status {
                add_event(state, team_id, &f.id, &status_line(&f, &o), None).await;
            } else if tier != f.tier {
                add_event(
                    state,
                    team_id,
                    &f.id,
                    &format!("Tier now {}", f.tier.map(|t| t.as_str()).unwrap_or("")),
                    None,
                )
                .await;
            }
        }
        out.changed.push(f.id.clone());
        match idx {
            Some(i) => findings[i] = (f, st),
            None => findings.push((f, st)),
        }
    }
    out
}

fn source_name(source: &str) -> &'static str {
    match source {
        "dependabot" => "a Dependabot alert",
        "github_advisory" => "a GitHub advisory",
        sec::REPOSITORY_ADVISORY => "the repository's own security advisory",
        _ => "OSV",
    }
}

fn status_line(f: &Finding, o: &Observation) -> String {
    match f.status {
        Status::Fixed => "Fixed: the Dependabot alert closed as fixed".to_string(),
        Status::Dismissed => format!(
            "Dismissed on GitHub{}",
            f.dismissed
                .as_ref()
                .map(|d| format!(" by {}: {}", d.by, d.reason))
                .unwrap_or_default()
        ),
        Status::Withdrawn => "The advisory was withdrawn".to_string(),
        Status::Open => format!(
            "Open again ({})",
            o.alert_state.as_deref().unwrap_or("the source reopened it")
        ),
        s => format!("Status now {}", s.as_str()),
    }
}

/// Fills a Dependabot observation's installed version from the inventory,
/// and how the manifest pulls it in.
fn fill_version(o: &mut Observation, deps: &[Dep]) {
    if o.version.is_empty() {
        if let Some(v) = sec::installed_version(deps, &o.ecosystem, &o.package, &o.manifest) {
            o.version = v;
        }
    }
    if o.path.is_empty() {
        if let Some(d) = deps.iter().find(|d| {
            d.ecosystem.eq_ignore_ascii_case(&o.ecosystem)
                && d.name == o.package
                && d.version == o.version
                && d.manifest == o.manifest
                && !d.path.is_empty()
        }) {
            o.path = d.path.clone();
            if o.relation == sec::Relation::Unknown {
                o.relation = relation_of(&d.path);
            }
        }
    }
}

/// Direct or transitive, from a dependency's path (unknown without one).
fn relation_of(path: &[String]) -> sec::Relation {
    match path.len() {
        0 => sec::Relation::Unknown,
        1 => sec::Relation::Direct,
        _ => sec::Relation::Transitive,
    }
}

// ── Sources ─────────────────────────────────────────────────────────────

/// Dependabot's open alerts for a repository (`pages` of 100 at most), and
/// what access says, in plain words.
async fn dependabot_alerts(
    gh: &GitHubClient,
    repo: &str,
    pages: u32,
) -> (Vec<Value>, sec::DependabotAccess) {
    let mut out = vec![];
    // The token first, so its refusal is told apart from GitHub not answering.
    if let Err(e) = gh.installation_token().await {
        let why = sec::DependabotUnreached::Token(e.to_string());
        warn!(repo, error = %e, "security: no GitHub token for Dependabot's alerts");
        return (out, sec::dependabot_unreached(&why));
    }
    // This endpoint pages by cursor; a `page` parameter is answered 400.
    let mut after: Option<String> = None;
    for _ in 0..pages.max(1) {
        let path = sec::dependabot_alerts_path(repo, after.as_deref());
        let (code, body, link) = match gh.get_status_link(&path).await {
            Ok(r) => r,
            Err(e) => {
                let why = unreached(e.as_ref());
                warn!(repo, error = %e, cause = ?why, "security: GitHub didn't answer for Dependabot's alerts");
                return (out, sec::dependabot_unreached(&why));
            }
        };
        if !(200..300).contains(&code) {
            warn!(
                repo,
                status = code,
                github = body["message"].as_str().unwrap_or(""),
                "security: GitHub turned down Dependabot's alerts"
            );
            let p = gh.permission_state("vulnerability_alerts").await;
            let mut access = sec::dependabot_access(code, &body, p.token_has, p.installation_lacks);
            if p.broker_unaware && access.state != "off" {
                access = sec::DependabotAccess {
                    state: "no_permission".into(),
                    reason: "CoderHelm's hosted token service doesn't pass Dependabot alerts to this install yet; it does from its next release, with nothing to change here.".into(),
                };
            }
            return (vec![], access);
        }
        out.extend(body.as_array().cloned().unwrap_or_default());
        after = sec::dependabot_next_cursor(link.as_deref());
        if after.is_none() {
            break;
        }
    }
    (
        out,
        sec::dependabot_access(200, &Value::Null, Some(true), false),
    )
}

/// What kept a request from getting any answer from GitHub.
fn unreached(e: &(dyn std::error::Error + Send + Sync + 'static)) -> sec::DependabotUnreached {
    match e.downcast_ref::<reqwest::Error>() {
        Some(r) if r.is_timeout() => sec::DependabotUnreached::TimedOut,
        Some(r) if r.is_connect() => sec::DependabotUnreached::Connect(error_chain(r)),
        Some(r) => sec::DependabotUnreached::Other(error_chain(r)),
        None => sec::DependabotUnreached::Other(e.to_string()),
    }
}

/// An error with its causes, without the URL.
fn error_chain(e: &reqwest::Error) -> String {
    let mut s = e.to_string();
    if let Some(u) = e.url() {
        s = s.replace(&format!(" for url ({u})"), "");
    }
    let mut src = std::error::Error::source(e);
    while let Some(c) = src {
        s.push_str(": ");
        s.push_str(&c.to_string());
        src = c.source();
    }
    s
}

/// The repository's own security advisories, with how they were read:
/// `ok`, `no_permission` (the installation hasn't granted Repository
/// security advisories; nothing is asked again until a token carries it) or
/// `error`.
async fn repo_advisories(gh: &GitHubClient, repo: &str) -> (Vec<Value>, &'static str) {
    match gh
        .get_granted("repository_advisories", &sec::repo_advisories_path(repo))
        .await
    {
        Ok(Some(v)) => (v.as_array().cloned().unwrap_or_default(), "ok"),
        Ok(None) => (Vec::new(), "no_permission"),
        Err(e) => {
            warn!(repo, error = %e, "security: could not read the repository's advisories");
            (Vec::new(), "error")
        }
    }
}

/// Saves what reading Dependabot said, for the repository and the team.
async fn record_dependabot(cx: &Ctx<'_>, repo: &str, access: &sec::DependabotAccess) {
    let now = Utc::now().to_rfc3339();
    set_repo_fields(
        cx.state,
        cx.team_id,
        repo,
        &[
            ("dependabot", attr_s(&access.state)),
            ("dependabot_reason", attr_s(&access.reason)),
            ("dependabot_checked_at", attr_s(&now)),
        ],
    )
    .await;
    set_team_state(
        cx.state,
        cx.team_id,
        &[
            ("dependabot", attr_s(&access.state)),
            ("dependabot_reason", attr_s(&access.reason)),
            ("dependabot_checked_at", attr_s(&now)),
            (
                "permission_missing",
                AttributeValue::Bool(matches!(
                    access.state.as_str(),
                    "no_permission" | "not_accepted"
                )),
            ),
        ],
    )
    .await;
    // The repository's own problem state: the team's row above is whichever
    // repository was read last, the page reads every repository's.
    let sk = sec::deps_sk(repo);
    let kind = match access.state.as_str() {
        "on" => None,
        "off" => Some(Kind::DependabotOff),
        "no_permission" | "not_accepted" => Some(Kind::DependabotNoPermission),
        _ => Some(Kind::DependabotReadFailed),
    };
    match kind {
        None => record_problem(cx.state, cx.team_id, &sk, Check::Dependabot, Outcome::Pass).await,
        Some(k) => {
            let why = if access.reason.is_empty() {
                "The last read of Dependabot's alerts failed; the next check tries again."
                    .to_string()
            } else {
                access.reason.clone()
            };
            record_problem(
                cx.state,
                cx.team_id,
                &sk,
                Check::Dependabot,
                Outcome::Fail(k, why),
            )
            .await
        }
    }
}

/// OSV's matches for some packages: `(index, vulnerability id)`, following
/// each result's `next_page_token` (a package with many records).
pub(super) async fn osv_query(
    state: &WorkerState,
    queries: Vec<Value>,
) -> Result<Vec<(usize, String)>, String> {
    let mut hits = vec![];
    let mut pending: Vec<(usize, Value)> = queries.into_iter().enumerate().collect();
    for _ in 0..10 {
        if pending.is_empty() {
            break;
        }
        let body = json!({ "queries": pending.iter().map(|(_, q)| q.clone()).collect::<Vec<_>>() });
        let resp = state
            .http
            .post(format!("{}/v1/querybatch", sec::OSV_API))
            .json(&body)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| format!("OSV didn't answer ({})", e.without_url()))?;
        let code = resp.status().as_u16();
        if !(200..300).contains(&code) {
            return Err(format!("OSV answered {code}"));
        }
        let v = resp
            .json::<Value>()
            .await
            .map_err(|_| "OSV's answer couldn't be read".to_string())?;
        let results = v["results"].as_array().cloned().unwrap_or_default();
        if results.len() != pending.len() {
            return Err("OSV's answer didn't match the question".into());
        }
        let mut next = vec![];
        for ((i, q), r) in pending.into_iter().zip(results) {
            for vuln in r["vulns"].as_array().into_iter().flatten() {
                if let Some(id) = vuln["id"].as_str() {
                    hits.push((i, id.to_string()));
                }
            }
            if let Some(tok) = r["next_page_token"].as_str().filter(|t| !t.is_empty()) {
                let mut q = q;
                q["page_token"] = json!(tok);
                next.push((i, q));
            }
        }
        pending = next;
    }
    Ok(hits)
}

/// OSV's records for a repository's dependencies, as observations. `cache`
/// holds the records read this tick (repositories share packages).
async fn osv_observations(
    state: &WorkerState,
    repo: &str,
    deps: &[Dep],
    cache: &mut HashMap<String, Value>,
) -> Result<Vec<Observation>, String> {
    use futures::StreamExt;
    let lookup: Vec<&Dep> = deps.iter().filter(|d| d.ecosystem != "docker").collect();
    let mut hits: Vec<(usize, String)> = vec![];
    for (chunk_i, chunk) in lookup.chunks(500).enumerate() {
        let owned: Vec<Dep> = chunk.iter().map(|d| (*d).clone()).collect();
        for (i, id) in osv_query(state, sec::osv_queries(&owned)).await? {
            hits.push((chunk_i * 500 + i, id));
        }
    }
    let mut seen = HashSet::new();
    let wanted: Vec<String> = hits
        .iter()
        .filter(|(_, id)| seen.insert(id.clone()))
        .map(|(_, id)| id.clone())
        .filter(|id| !cache.contains_key(id))
        .collect();
    let room = OSV_HYDRATE_MAX.saturating_sub(cache.len());
    if wanted.len() > room {
        warn!(
            repo,
            wanted = wanted.len(),
            room,
            "security: more OSV records than one sync reads; the rest are read on the next sync"
        );
    }
    let fetched: Vec<(String, Option<Value>)> =
        futures::stream::iter(wanted.into_iter().take(room))
            .map(|id| async move {
                let url = format!("{}/v1/vulns/{}", sec::OSV_API, urlencoding::encode(&id));
                let v = match state
                    .http
                    .get(url)
                    .timeout(std::time::Duration::from_secs(20))
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
                    _ => None,
                };
                (id, v)
            })
            .buffer_unordered(OSV_PARALLEL)
            .collect()
            .await;
    for (id, v) in fetched {
        if let Some(v) = v {
            cache.insert(id, v);
        }
    }
    let mut out = vec![];
    for (i, id) in hits {
        let (Some(d), Some(v)) = (lookup.get(i), cache.get(&id)) else {
            continue;
        };
        let Some(adv) = sec::advisory_from_osv(v, &d.ecosystem, &d.name) else {
            continue;
        };
        out.push(Observation {
            repo: repo.to_string(),
            manifest: d.manifest.clone(),
            ecosystem: d.ecosystem.clone(),
            package: d.name.clone(),
            version: d.version.clone(),
            advisory: adv,
            scope: d.scope,
            relation: relation_of(&d.path),
            path: d.path.clone(),
            ..Default::default()
        });
    }
    Ok(out)
}

/// The CISA list, read once a day for everyone.
async fn kev(state: &WorkerState) -> BTreeMap<String, String> {
    let day = Utc::now().format("%Y%m%d").to_string();
    let table = &state.config.settings_table_name;
    if claim(
        state,
        sec::GLOBAL_PK,
        &format!("KEV_SYNC#{day}"),
        2 * 86_400,
    )
    .await
    {
        let fetched = async {
            let r = state
                .http
                .get(sec::KEV_URL)
                .timeout(std::time::Duration::from_secs(30))
                .send()
                .await
                .ok()?;
            r.json::<Value>().await.ok()
        }
        .await;
        match fetched.map(|v| sec::kev_map(&v)).filter(|m| !m.is_empty()) {
            Some(map) => {
                let text = serde_json::to_string(&map).unwrap_or_default();
                let _ = state
                    .dynamo
                    .put_item()
                    .table_name(table)
                    .item("pk", attr_s(sec::GLOBAL_PK))
                    .item("sk", attr_s(sec::KEV_SK))
                    .item("map", attr_s(&text))
                    .item("count", attr_n(map.len()))
                    .item("synced_at", attr_s(&Utc::now().to_rfc3339()))
                    .send()
                    .await;
                return map;
            }
            None => {
                warn!("security: the CISA list could not be read; trying again next tick");
                release(state, sec::GLOBAL_PK, &format!("KEV_SYNC#{day}")).await;
            }
        }
    }
    state
        .dynamo
        .get_item()
        .table_name(table)
        .key("pk", attr_s(sec::GLOBAL_PK))
        .key("sk", attr_s(sec::KEV_SK))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .and_then(|i| serde_json::from_str(&s(&i, "map")).ok())
        .unwrap_or_default()
}

/// FIRST's EPSS scores for the CVEs of open findings.
/// FIRST's scores for these CVEs, and why a batch couldn't be read (the
/// scores read before it are kept).
async fn epss(
    state: &WorkerState,
    cves: &[String],
) -> (HashMap<String, (f64, f64)>, Option<String>) {
    let mut out = HashMap::new();
    let mut failed = None;
    for batch in sec::epss_batches(cves) {
        let url = format!("{}?cve={}", sec::EPSS_API, batch);
        let r = match state
            .http
            .get(url)
            .timeout(std::time::Duration::from_secs(20))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                failed = Some(format!("FIRST answered {}", r.status().as_u16()));
                continue;
            }
            Err(e) => {
                failed = Some(if e.is_timeout() {
                    "FIRST didn't answer in time".to_string()
                } else {
                    "FIRST couldn't be reached".to_string()
                });
                continue;
            }
        };
        match r.json::<Value>().await {
            Ok(v) => out.extend(sec::epss_map(&v)),
            Err(_) => failed = Some("FIRST's answer couldn't be read".to_string()),
        }
    }
    (out, failed)
}

// ── Triage ──────────────────────────────────────────────────────────────

fn triage_key(f: &Finding) -> String {
    common::content_hash(&format!(
        "{TRIAGE_VERSION}|{}|{}|{:?}",
        f.advisory.ids.join(","),
        f.version,
        f.scope
    ))
}

/// The code lines that mention the search terms: code search for the
/// files, then each file read for its line numbers. Also the number of
/// files that matched. An error when a search failed (then nothing can be
/// concluded from "no matches").
async fn code_hits(
    gh: &GitHubClient,
    repo: &str,
    terms: &[String],
) -> Result<(Vec<EvidenceLine>, usize), Error> {
    let (owner, name) = repo.split_once('/').ok_or("bad repo")?;
    let branch = gh.get_default_branch(owner, name).await?;
    let mut files: Vec<(String, String)> = vec![];
    for t in terms {
        // No quotes: the snapshot's search takes them literally, so
        // `"axios"` never matched `from 'axios'`.
        let results = gh.search_code(owner, name, &branch, t).await?;
        for r in results.into_iter().take(4) {
            if is_lockfile_or_manifest(&r.path) {
                continue;
            }
            if !files.iter().any(|(p, _)| *p == r.path) {
                files.push((r.path, t.clone()));
            }
        }
    }
    let mut out = vec![];
    for (path, _) in files.iter().take(6) {
        let Ok(text) = gh.read_file(owner, name, path, &branch).await else {
            continue;
        };
        for (i, line) in text.lines().enumerate() {
            if terms.iter().any(|t| line.contains(t.as_str())) {
                out.push(EvidenceLine {
                    file: path.clone(),
                    line: i as u64 + 1,
                    text: common::truncate_str(line.trim(), 240).to_string(),
                });
                if out.len() >= 30 {
                    return Ok((out, files.len()));
                }
            }
        }
    }
    Ok((out, files.len()))
}

fn is_lockfile_or_manifest(path: &str) -> bool {
    sec::is_dependency_file(path) || path.ends_with(".lock") || path.ends_with(".sum")
}

/// Today's triage spend for the team: adds `add` tokens and `packages`
/// packages checked, and returns today's tokens. Kept 400 days (Analytics).
async fn triage_tokens(state: &WorkerState, team_id: &str, add: u64, packages: u64) -> u64 {
    let day = Utc::now().format("%Y%m%d").to_string();
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("SECTRIAGE#{day}")))
        .update_expression("ADD tokens :n, packages :p SET #ttl = :ttl")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":n", attr_n(add))
        .expression_attribute_values(":p", attr_n(packages))
        .expression_attribute_values(":ttl", attr_n(Utc::now().timestamp() + 400 * 86_400))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
        .send()
        .await;
    r.ok()
        .and_then(|o| o.attributes().and_then(|a| a.get("tokens").cloned()))
        .and_then(|v| v.as_n().ok().and_then(|n| n.parse().ok()))
        .unwrap_or(u64::MAX)
}

/// How one group's triage went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Triaged {
    /// The same advisories and the same code lines as last time: that verdict.
    Reused,
    /// Settled without the model (dev only, withdrawn, no code names it).
    Settled,
    /// Settled by reading the code (imports, calls, the lockfile's parents).
    Code(Verdict),
    /// One model call.
    Model,
    /// No model call left this tick, or today's limit is used.
    Waiting,
    /// The search or the model failed; tried again next tick.
    Failed,
}

fn group_verdict_sk(group_key: &str) -> String {
    format!("SECGROUPV#{}", common::content_hash(group_key))
}

async fn load_group_verdict(cx: &Ctx<'_>, gk: &str, key: &str) -> Option<(Verdict, Evidence)> {
    let i = cx
        .state
        .dynamo
        .get_item()
        .table_name(&cx.state.config.settings_table_name)
        .key("pk", attr_s(cx.team_id))
        .key("sk", attr_s(&group_verdict_sk(gk)))
        .send()
        .await
        .ok()?
        .item()
        .cloned()?;
    if s(&i, "key") != key {
        return None;
    }
    Some((
        serde_json::from_str(&s(&i, "verdict")).ok()?,
        serde_json::from_str(&s(&i, "evidence")).ok()?,
    ))
}

async fn save_group_verdict(cx: &Ctx<'_>, gk: &str, key: &str, v: Verdict, e: &Evidence) {
    let _ = cx
        .state
        .dynamo
        .put_item()
        .table_name(&cx.state.config.settings_table_name)
        .item("pk", attr_s(cx.team_id))
        .item("sk", attr_s(&group_verdict_sk(gk)))
        .item("key", attr_s(key))
        .item(
            "verdict",
            attr_s(&serde_json::to_string(&v).unwrap_or_default()),
        )
        .item(
            "evidence",
            attr_s(&serde_json::to_string(e).unwrap_or_default()),
        )
        .item("checked_at", attr_s(&Utc::now().to_rfc3339()))
        .item("ttl", attr_n(Utc::now().timestamp() + 90 * 86_400))
        .send()
        .await;
}

/// Gives every member of a group the verdict, saves them and notes it.
async fn apply_verdict(
    cx: &Ctx<'_>,
    findings: &mut [(Finding, FindingState)],
    idxs: &[usize],
    v: Verdict,
    e: &Evidence,
) {
    for i in idxs {
        let (f, st) = &mut findings[*i];
        let changed = f.verdict != v;
        f.verdict = v;
        f.evidence = Some(e.clone());
        f.waiting = None;
        st.triaged = Some(triage_key(f));
        st.triaged_at = Some(Utc::now().to_rfc3339());
        sec::retier(f, st, cx.cfg);
        f.updated_at = Utc::now().to_rfc3339();
        save_finding(cx.state, cx.team_id, f, st).await;
        if changed {
            add_event(
                cx.state,
                cx.team_id,
                &f.id,
                &format!("Verdict: {}", verdict_word(v)),
                None,
            )
            .await;
        }
    }
}

/// Marks a group's members as waiting for tomorrow's check.
async fn mark_waiting(
    cx: &Ctx<'_>,
    findings: &mut [(Finding, FindingState)],
    idxs: &[usize],
    why: &str,
) {
    for i in idxs {
        let (f, st) = &mut findings[*i];
        if f.waiting.as_deref() == Some(why) {
            continue;
        }
        f.waiting = Some(why.to_string());
        st.triaged = None;
        save_finding(cx.state, cx.team_id, f, st).await;
    }
}

/// Works out one group's verdict (a package at one version, every advisory
/// it has): one code search, then an earlier verdict for the same code, a
/// verdict without the model, or one model call for the whole group.
async fn triage_group(
    cx: &Ctx<'_>,
    findings: &mut [(Finding, FindingState)],
    idxs: &[usize],
    calls_left: usize,
) -> Triaged {
    let Some(&lead_i) = idxs.iter().min_by_key(|i| findings[**i].0.tier) else {
        return Triaged::Failed;
    };
    let lead = findings[lead_i].0.clone();
    let gk = sec::group_key(&lead);
    // What every member's advisories name, together.
    let mut st_all = findings[lead_i].1.clone();
    for i in idxs {
        for sym in &findings[*i].1.symbols {
            if !st_all.symbols.contains(sym) {
                st_all.symbols.push(sym.clone());
            }
        }
    }
    if lead.scope == Scope::Dev {
        if let Some((v, e)) = sec::settle_without_model(&lead, &st_all, 1, 1, &[]) {
            apply_verdict(cx, findings, idxs, v, &e).await;
            return Triaged::Settled;
        }
    }
    let mut terms: Vec<String> = vec![];
    for i in idxs {
        for t in sec::search_terms(&findings[*i].0, &findings[*i].1) {
            if !terms.contains(&t) {
                terms.push(t);
            }
        }
    }
    terms.truncate(6);
    let (hits, files) = match code_hits(&cx.gh, &lead.repo, &terms).await {
        Ok(h) => h,
        Err(e) => {
            warn!(team_id = cx.team_id, repo = %lead.repo, error = %e, "security: the code search failed; triage is tried again next tick");
            return Triaged::Failed;
        }
    };
    let members: Vec<&Finding> = idxs.iter().map(|i| &findings[*i].0).collect();
    let key = sec::group_triage_key(&members, &hits);
    if let Some((v, e)) = load_group_verdict(cx, &gk, &key).await {
        apply_verdict(cx, findings, idxs, v, &e).await;
        return Triaged::Reused;
    }
    if let Some((v, e)) = sec::settle_without_model(&lead, &st_all, hits.len(), files, &terms) {
        save_group_verdict(cx, &gk, &key, v, &e).await;
        apply_verdict(cx, findings, idxs, v, &e).await;
        return Triaged::Settled;
    }
    if calls_left == 0 {
        return Triaged::Waiting;
    }
    let checked = format!(
        "Searched the code for {}: {} file{}, {} matching line{}.",
        terms
            .iter()
            .map(|t| format!("`{t}`"))
            .collect::<Vec<_>>()
            .join(", "),
        files,
        if files == 1 { "" } else { "s" },
        hits.len(),
        if hits.len() == 1 { "" } else { "s" }
    );
    if let Some(why) = super::check_budget_exceeded(cx.state, cx.team_id).await {
        let e = Evidence {
            summary: format!("Not read by the agent: {why}."),
            lines: hits.into_iter().take(5).collect(),
            checked,
            confidence: "low".into(),
        };
        mark_waiting(cx, findings, idxs, &e.summary).await;
        return Triaged::Waiting;
    }
    if triage_tokens(cx.state, cx.team_id, 0, 0).await >= cx.cfg.triage_tokens_per_day {
        mark_waiting(cx, findings, idxs, sec::WAITING_TRIAGE).await;
        return Triaged::Waiting;
    }
    let Ok(provider) = crate::agent::provider::ModelProvider::load_for_team(
        &cx.state.dynamo,
        &cx.state.config.settings_table_name,
        cx.team_id,
    )
    .await
    else {
        let e = Evidence {
            summary: "No model is set up for this team, so only the code search ran.".into(),
            lines: hits.into_iter().take(5).collect(),
            checked,
            confidence: "low".into(),
        };
        apply_verdict(cx, findings, idxs, Verdict::Unknown, &e).await;
        return Triaged::Settled;
    };
    let mut prompt = sec::triage_prompt(&lead, &st_all, &hits, TRIAGE_MAX_CHARS);
    if idxs.len() > 1 {
        prompt.push_str("\n\nThe same package and version also has these advisories; one verdict covers them all:\n");
        for i in idxs.iter().filter(|i| **i != lead_i).take(20) {
            let f = &findings[*i].0;
            prompt.push_str(&format!(
                "- {}: {}\n",
                f.advisory.primary,
                common::truncate_str(&f.advisory.title, 160)
            ));
        }
    }
    let mut usage = TokenUsage::default();
    let answer = crate::agent::provider::converse_simple(
        cx.state,
        &provider,
        provider.primary_model_id(),
        sec::TRIAGE_SYSTEM,
        &prompt,
        &mut usage,
    )
    .await;
    triage_tokens(cx.state, cx.team_id, usage.total_tokens(), 1).await;
    match answer {
        Ok(text) => {
            let (v, mut e) = sec::parse_triage(&text, &hits);
            e.checked = checked;
            if v == Verdict::Unknown {
                e.summary = format!(
                    "{} {}",
                    sec::unknown_reason(&lead.package, files, &st_all.symbols),
                    e.summary
                );
            }
            save_group_verdict(cx, &gk, &key, v, &e).await;
            apply_verdict(cx, findings, idxs, v, &e).await;
            Triaged::Model
        }
        Err(err) => {
            warn!(team_id = cx.team_id, group = %gk, error = %err, "security: triage call failed");
            Triaged::Failed
        }
    }
}

/// Whether a finding's verdict is read on this tick: never read, read by an
/// older way of reading the code, its advisories or version changed, or
/// read more than [`TRIAGE_RECHECK_DAYS`] ago.
fn triage_due(f: &Finding, st: &FindingState, now: chrono::DateTime<Utc>) -> bool {
    if f.verdict == Verdict::Pending || st.triaged.as_deref() != Some(triage_key(f).as_str()) {
        return true;
    }
    st.triaged_at
        .as_deref()
        .and_then(sec::parse_time)
        .is_none_or(|t| now - t >= Duration::days(TRIAGE_RECHECK_DAYS))
}

/// The groups to read, the most urgent first: the lowest tier, then the
/// highest score (criticals before highs), runtime before scope unknown
/// before dev.
fn triage_order(
    findings: &[(Finding, FindingState)],
    groups: &BTreeMap<String, Vec<usize>>,
) -> Vec<String> {
    let scope_rank = |s: sec::Scope| match s {
        sec::Scope::Runtime => 0,
        sec::Scope::Unknown => 1,
        sec::Scope::Dev => 2,
    };
    let mut order: Vec<(Option<sec::Tier>, i64, u8, String)> = groups
        .iter()
        .map(|(k, idxs)| {
            let tier = idxs.iter().filter_map(|i| findings[*i].0.tier).min();
            let score = idxs
                .iter()
                .map(|i| (sec::score(&findings[*i].0, &findings[*i].1) * 10.0) as i64)
                .max()
                .unwrap_or(0);
            let scope = idxs
                .iter()
                .map(|i| scope_rank(findings[*i].0.scope))
                .min()
                .unwrap_or(1);
            (tier, -score, scope, k.clone())
        })
        .collect();
    order.sort();
    order.into_iter().map(|(_, _, _, k)| k).collect()
}

/// The lockfiles that say how a manifest's packages come in: the manifest
/// itself when it is one; for a `package.json`, the nearest lockfile in its
/// folder or above; for the dependency graph's list, every lockfile.
fn lockfiles_for(manifest: &str, tree: &[String]) -> Vec<String> {
    use common::reachability as reach;
    let in_tree = |p: &str| tree.iter().any(|t| t == p);
    if reach::is_lockfile(manifest) {
        return if in_tree(manifest) {
            vec![manifest.to_string()]
        } else {
            vec![]
        };
    }
    if manifest == sec::SBOM_MANIFEST {
        return tree
            .iter()
            .filter(|t| reach::is_lockfile(t) && !t.contains("node_modules/"))
            .cloned()
            .collect();
    }
    let mut dir = reach::Query::dir_of(manifest);
    loop {
        let found: Vec<String> = reach::LOCKFILES
            .iter()
            .map(|n| format!("{dir}{n}"))
            .filter(|p| in_tree(p))
            .collect();
        if !found.is_empty() || dir.is_empty() {
            return found;
        }
        let trimmed = dir.trim_end_matches('/');
        dir = match trimmed.rsplit_once('/') {
            Some((up, _)) => format!("{up}/"),
            None => String::new(),
        };
    }
}

/// Reads the code once for a repository's groups: one snapshot (a tarball),
/// the lockfiles, then one pass over every file. A decision per group, in
/// order; None when the snapshot can't be had (the search path runs then).
async fn code_decisions(
    cx: &Ctx<'_>,
    repo: &str,
    groups: &[Vec<usize>],
    findings: &[(Finding, FindingState)],
) -> Option<Vec<common::reachability::Decision>> {
    use common::reachability as reach;
    let (owner, name) = repo.split_once('/')?;
    let branch = cx.gh.get_default_branch(owner, name).await.ok()?;
    let snap = cx.gh.code_snapshot(owner, name, &branch).await?;
    let tree = snap.tree().await;
    let mut locks: HashMap<String, Option<String>> = HashMap::new();
    let mut queries = vec![];
    for idxs in groups {
        let lead = &findings[idxs[0]].0;
        let mut symbols: Vec<String> = vec![];
        let mut hints: Vec<String> = vec![];
        for i in idxs {
            let (f, st) = &findings[*i];
            for s in &st.symbols {
                if !symbols.contains(s) {
                    symbols.push(s.clone());
                }
            }
            let text = format!("{} {}", f.advisory.title, f.advisory.summary);
            for h in reach::hints_from_text(&text, &f.package) {
                if !hints.contains(&h) && hints.len() < 12 {
                    hints.push(h);
                }
            }
        }
        // What brings it in, from the lockfile (npm, pnpm, Yarn).
        let mut ancestry = reach::Ancestry::default();
        let lockfiles = if lead.ecosystem == "npm" {
            lockfiles_for(&lead.manifest, &tree)
        } else {
            vec![]
        };
        for lf in &lockfiles {
            if !locks.contains_key(lf) {
                let text = match snap.read_file(lf).await {
                    Some(t) => Some(t),
                    None => cx.gh.read_file(owner, name, lf, &branch).await.ok(),
                };
                locks.insert(lf.clone(), text);
            }
            let Some(Some(text)) = locks.get(lf) else {
                continue;
            };
            let direct_names = if lf.ends_with("yarn.lock") {
                let dir = reach::Query::dir_of(lf);
                let mut names = vec![];
                for p in tree.iter().filter(|p| {
                    p.starts_with(&dir)
                        && p.ends_with("package.json")
                        && !p.contains("node_modules/")
                }) {
                    if let Some(t) = snap.read_file(p).await {
                        names.extend(reach::package_json_names(&t));
                    }
                }
                names
            } else {
                vec![]
            };
            if let Some(a) =
                reach::lock_ancestry(lf, text, &lead.package, &lead.version, &direct_names)
            {
                ancestry.merge(a);
            }
        }
        let (parents, parents_unknown) = if ancestry.found {
            if ancestry.direct {
                (vec![], false)
            } else {
                let p: Vec<String> = ancestry.parents.iter().cloned().collect();
                let unknown = p.is_empty();
                (p, unknown)
            }
        } else if let Some(p) = sec::direct_parent(&lead.path) {
            (vec![p.to_string()], false)
        } else {
            let direct = lead.relation == sec::Relation::Direct || lead.path.len() == 1;
            (vec![], !direct)
        };
        let dir = match lockfiles.as_slice() {
            [one] => reach::Query::dir_of(one),
            _ => reach::Query::dir_of(&lead.manifest),
        };
        queries.push(reach::Query {
            ecosystem: lead.ecosystem.clone(),
            package: lead.package.clone(),
            dir,
            parents,
            symbols,
            hints,
            parents_unknown,
        });
    }
    let mut scanner = reach::Scanner::new(queries);
    snap.visit(&mut |path, text| scanner.add(path, text)).await;
    Some(
        scanner
            .finish()
            .iter()
            .map(|(q, facts)| reach::decide(q, facts))
            .collect(),
    )
}

/// One group's verdict from the code check: the verdict it settled, or the
/// model reading the lines it found (an earlier verdict for the same lines
/// is reused).
async fn triage_decided(
    cx: &Ctx<'_>,
    findings: &mut [(Finding, FindingState)],
    idxs: &[usize],
    decision: common::reachability::Decision,
    calls_left: usize,
) -> Triaged {
    use common::reachability::Decision;
    let Some(&lead_i) = idxs.iter().min_by_key(|i| findings[**i].0.tier) else {
        return Triaged::Failed;
    };
    let lead = findings[lead_i].0.clone();
    if lead.scope == sec::Scope::Dev || lead.status == Status::Withdrawn {
        let st = findings[lead_i].1.clone();
        if let Some((v, e)) = sec::settle_without_model(&lead, &st, 1, 1, &[]) {
            apply_verdict(cx, findings, idxs, v, &e).await;
            return Triaged::Settled;
        }
    }
    let (evidence, found) = match decision {
        Decision::Settled(v, e) => {
            apply_verdict(cx, findings, idxs, v, &e).await;
            return Triaged::Code(v);
        }
        Decision::Ask(e, found) => (e, found),
    };
    let gk = sec::group_key(&lead);
    let members: Vec<&Finding> = idxs.iter().map(|i| &findings[*i].0).collect();
    let key = sec::group_triage_key(&members, &evidence.lines);
    let key = common::content_hash(&format!("{TRIAGE_VERSION}|{key}"));
    if let Some((v, e)) = load_group_verdict(cx, &gk, &key).await {
        apply_verdict(cx, findings, idxs, v, &e).await;
        return Triaged::Reused;
    }
    if calls_left == 0 {
        return Triaged::Waiting;
    }
    let mut st_all = findings[lead_i].1.clone();
    for i in idxs {
        for sym in &findings[*i].1.symbols {
            if !st_all.symbols.contains(sym) {
                st_all.symbols.push(sym.clone());
            }
        }
    }
    if let Some(why) = super::check_budget_exceeded(cx.state, cx.team_id).await {
        mark_waiting(
            cx,
            findings,
            idxs,
            &format!("Not read by the agent: {why}."),
        )
        .await;
        return Triaged::Waiting;
    }
    let Ok(provider) = crate::agent::provider::ModelProvider::load_for_team(
        &cx.state.dynamo,
        &cx.state.config.settings_table_name,
        cx.team_id,
    )
    .await
    else {
        let e = Evidence {
            summary: format!(
                "{found} No model is set up for this team, so only the code check ran."
            ),
            ..evidence
        };
        apply_verdict(cx, findings, idxs, Verdict::Unknown, &e).await;
        return Triaged::Settled;
    };
    let mut prompt = format!("What a check of every source file found: {found}\n\n");
    prompt.push_str(&sec::triage_prompt(
        &lead,
        &st_all,
        &evidence.lines,
        TRIAGE_MAX_CHARS,
    ));
    if idxs.len() > 1 {
        prompt.push_str("\n\nThe same package and version also has these advisories; one verdict covers them all:\n");
        for i in idxs.iter().filter(|i| **i != lead_i).take(20) {
            let f = &findings[*i].0;
            prompt.push_str(&format!(
                "- {}: {}\n",
                f.advisory.primary,
                common::truncate_str(&f.advisory.title, 160)
            ));
        }
    }
    let mut usage = TokenUsage::default();
    let answer = crate::agent::provider::converse_simple(
        cx.state,
        &provider,
        provider.primary_model_id(),
        sec::TRIAGE_SYSTEM,
        &prompt,
        &mut usage,
    )
    .await;
    triage_tokens(cx.state, cx.team_id, usage.total_tokens(), 1).await;
    match answer {
        Ok(text) => {
            let (v, mut e) = sec::parse_triage(&text, &evidence.lines);
            e.checked = evidence.checked.clone();
            if v == Verdict::Unknown {
                e.summary = format!("{found} {}", e.summary);
            }
            save_group_verdict(cx, &gk, &key, v, &e).await;
            apply_verdict(cx, findings, idxs, v, &e).await;
            Triaged::Model
        }
        Err(err) => {
            warn!(team_id = cx.team_id, group = %gk, error = %err, "security: triage call failed");
            Triaged::Failed
        }
    }
}

/// Triage for the tick: the groups whose verdict is missing or stale, the
/// most urgent first. The code is read once per repository and settles
/// what it can; the model reads the rest, as many as the day's token cap
/// leaves room for. Returns how many groups got a verdict.
async fn triage_tick(cx: &Ctx<'_>, findings: &mut [(Finding, FindingState)]) -> usize {
    let now = Utc::now();
    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (i, (f, st)) in findings.iter().enumerate() {
        // Base images are not searched in code (common::base_images).
        if f.status.is_open()
            && f.ecosystem != common::base_images::ECOSYSTEM
            && cx.cfg.repos.includes(&f.repo)
            && triage_due(f, st, now)
        {
            groups.entry(sec::group_key(f)).or_default().push(i);
        }
    }
    if groups.is_empty() {
        return 0;
    }
    let order: Vec<String> = triage_order(findings, &groups)
        .into_iter()
        .take(TRIAGE_SEARCH_PER_TICK)
        .collect();
    // The model calls the day's cap leaves room for (an unreadable counter
    // reads as the cap reached).
    let today = triage_tokens(cx.state, cx.team_id, 0, 0).await;
    let mut calls_left = common::reachability::model_calls_allowed(
        today,
        cx.cfg.triage_tokens_per_day,
        common::reachability::estimate_call_tokens(TRIAGE_MAX_CHARS),
        TRIAGE_PER_TICK,
    );
    // By repository, in the order of each one's most urgent group.
    let mut repos: Vec<(String, Vec<String>)> = vec![];
    for k in order {
        let repo = findings[groups[&k][0]].0.repo.to_ascii_lowercase();
        match repos.iter_mut().find(|(r, _)| *r == repo) {
            Some((_, ks)) => ks.push(k),
            None => repos.push((repo, vec![k])),
        }
    }
    let mut done = 0;
    let mut tally: BTreeMap<&'static str, usize> = BTreeMap::new();
    for (_, keys) in repos {
        if cx.out_of_time() {
            break;
        }
        let idx_lists: Vec<Vec<usize>> = keys.iter().map(|k| groups[k].clone()).collect();
        let repo = findings[idx_lists[0][0]].0.repo.clone();
        let decisions = code_decisions(cx, &repo, &idx_lists, findings).await;
        if decisions.is_none() {
            warn!(team_id = cx.team_id, repo = %repo, "security: the repository's files couldn't be read; triage searches the code instead");
        }
        let mut decisions = decisions.map(|d| d.into_iter());
        for idxs in idx_lists {
            if cx.out_of_time() {
                break;
            }
            let r = match decisions.as_mut().and_then(|d| d.next()) {
                Some(d) => triage_decided(cx, findings, &idxs, d, calls_left).await,
                None => triage_group(cx, findings, &idxs, calls_left).await,
            };
            *tally
                .entry(match r {
                    Triaged::Reused => "reused",
                    Triaged::Settled => "settled",
                    Triaged::Code(Verdict::Reachable) => "code_reachable",
                    Triaged::Code(Verdict::NotReachable) => "code_not_reachable",
                    Triaged::Code(Verdict::NotLoaded) => "code_not_loaded",
                    Triaged::Code(_) => "code_unknown",
                    Triaged::Model => "model",
                    Triaged::Waiting => "waiting",
                    Triaged::Failed => "failed",
                })
                .or_default() += 1;
            match r {
                Triaged::Model => {
                    calls_left = calls_left.saturating_sub(1);
                    done += 1;
                }
                Triaged::Reused | Triaged::Settled | Triaged::Code(_) => done += 1,
                Triaged::Waiting | Triaged::Failed => {}
            }
        }
    }
    if !tally.is_empty() {
        info!(team_id = cx.team_id, ?tally, calls_left, "security: triage");
    }
    done
}

// ── Cards ───────────────────────────────────────────────────────────────

pub(crate) async fn team_channels(state: &WorkerState, team_id: &str) -> common::notify::Channels {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(common::alert_notify::TEAM_SETTINGS_SK))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .unwrap_or_default();
    let enabled = item
        .get("enabled")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    if !enabled {
        return Default::default();
    }
    let cipher = common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: "",
    };
    common::notify::store::open_all(&cipher, team_id, &item).await
}

fn finding_url(state: &WorkerState, id: &str) -> String {
    format!(
        "{}/security/detail?id={id}",
        super::skills::app_url(&state.config.stage)
    )
}

/// Posts one card, once ever for `(thing, stage)`. True when it is posted
/// now or was before.
async fn post_card(
    state: &WorkerState,
    team_id: &str,
    channels: &common::notify::Channels,
    thing: &str,
    stage: Stage,
    f: &Finding,
) -> bool {
    if channels.is_empty() {
        return false;
    }
    let sk = sec::card_claim_sk(thing, stage.key());
    if !claim(state, team_id, &sk, sec::CARD_CLAIM_SECS).await {
        return true;
    }
    let msg = sec::card(f, stage, &finding_url(state, &f.id), Utc::now());
    let sent = common::notify::deliver_recorded(
        &common::notify::Http(&state.http),
        channels,
        &msg,
        "security",
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await;
    if !sent.any_ok() {
        release(state, team_id, &sk).await;
        return false;
    }
    true
}

// ── The team's work ─────────────────────────────────────────────────────

/// What one team's tick did.
#[derive(Debug, Default)]
pub struct TeamReport {
    /// Findings brought in line with their pull request or run (`release`).
    pub released: usize,
    pub repos_synced: usize,
    pub new_findings: usize,
    pub triaged: usize,
    pub runs: usize,
    pub cards: usize,
}

struct Ctx<'a> {
    state: &'a WorkerState,
    team_id: &'a str,
    cfg: &'a SecurityConfig,
    gh: GitHubClient,
    started: Instant,
    /// OSV records read this tick (repositories share packages).
    osv: tokio::sync::Mutex<HashMap<String, Value>>,
}

impl Ctx<'_> {
    fn out_of_time(&self) -> bool {
        self.started.elapsed().as_secs() >= TICK_BUDGET_SECS
    }
}

/// Inventory, Dependabot and OSV for one repository.
async fn sync_repo(
    cx: &Ctx<'_>,
    repo: &str,
    findings: &mut Vec<(Finding, FindingState)>,
    with_alerts: bool,
) -> usize {
    let deps = match refresh_inventory(cx.state, cx.team_id, &cx.gh, repo).await {
        Ok(d) => d,
        Err(e) => {
            warn!(team_id = cx.team_id, repo, error = %e, "security: inventory failed");
            load_deps(cx.state, cx.team_id, repo)
                .await
                .map(|(d, _)| d)
                .unwrap_or_default()
        }
    };
    let mut obs: Vec<Observation> = vec![];
    if with_alerts {
        let (alerts, access) = dependabot_alerts(&cx.gh, repo, 5).await;
        record_dependabot(cx, repo, &access).await;
        for a in &alerts {
            if let Some(mut o) = sec::observation_from_alert(repo, a) {
                fill_version(&mut o, &deps);
                obs.push(o);
            }
        }
        let (advisories, access) = repo_advisories(&cx.gh, repo).await;
        set_team_state(
            cx.state,
            cx.team_id,
            &[("repository_advisories", attr_s(access))],
        )
        .await;
        obs.extend(
            advisories
                .iter()
                .filter_map(|a| sec::observation_from_repo_advisory(repo, a)),
        );
    }
    let osv = {
        let mut cache = cx.osv.lock().await;
        osv_observations(cx.state, repo, &deps, &mut cache).await
    };
    // Dockerfile base images: their package lists against OSV's distribution data.
    let (base_obs, base_reads) = {
        let mut cache = cx.osv.lock().await;
        super::base_images::sync(cx.state, repo, &deps, &mut cache).await
    };
    obs.extend(base_obs);
    let now = Utc::now().to_rfc3339();
    match osv {
        Ok(o) => {
            set_repo_fields(
                cx.state,
                cx.team_id,
                repo,
                &[("osv_synced_at", attr_s(&now)), ("osv_error", attr_s(""))],
            )
            .await;
            set_team_state(
                cx.state,
                cx.team_id,
                &[("osv_synced_at", attr_s(&now)), ("osv_error", attr_s(""))],
            )
            .await;
            record_problem(
                cx.state,
                cx.team_id,
                &sec::deps_sk(repo),
                Check::Osv,
                Outcome::Pass,
            )
            .await;
            obs.extend(o);
        }
        Err(why) => {
            warn!(team_id = cx.team_id, repo, error = %why, "security: OSV couldn't be read");
            set_repo_fields(cx.state, cx.team_id, repo, &[("osv_error", attr_s(&why))]).await;
            set_team_state(
                cx.state,
                cx.team_id,
                &[("osv_error", attr_s(&why)), ("osv_error_at", attr_s(&now))],
            )
            .await;
            record_problem(
                cx.state,
                cx.team_id,
                &sec::deps_sk(repo),
                Check::Osv,
                Outcome::Fail(
                    Kind::OsvReadFailed,
                    format!("{why}; tried again on the next check."),
                ),
            )
            .await;
        }
    }
    let applied = upsert(cx.state, cx.team_id, cx.cfg, findings, obs).await;
    base_images_settle(cx, repo, findings, &base_reads).await;
    // The dependency list now shows some fixed: a merged bump, a removal.
    for (f, st) in findings.iter_mut() {
        if f.repo.eq_ignore_ascii_case(repo)
            && f.status.is_open()
            && f.ecosystem != "docker"
            && sec::fixed_by_inventory(f, &deps)
        {
            f.status = Status::Fixed;
            f.updated_at = Utc::now().to_rfc3339();
            if let Some(p) = &mut f.pr {
                if p.state == "open" {
                    p.state = "merged".into();
                }
            }
            if st.merged_at.is_none() && f.pr.is_some() {
                st.merged_at = Some(f.updated_at.clone());
            }
            save_finding(cx.state, cx.team_id, f, st).await;
            add_event(
                cx.state,
                cx.team_id,
                &f.id,
                "Fixed: the dependency list no longer has a vulnerable version",
                None,
            )
            .await;
        }
    }
    applied.created
}

/// Base image findings after a read: fixed when the image no longer has
/// the version, else their pinned-digest fix; never triaged in code.
async fn base_images_settle(
    cx: &Ctx<'_>,
    repo: &str,
    findings: &mut [(Finding, FindingState)],
    reads: &[common::base_images::Resolution],
) {
    use common::base_images::{self as bi, Change};
    if !reads.is_empty() {
        let summary: Vec<Value> = reads
            .iter()
            .map(|r| json!({ "manifest": r.manifest, "image": format!("{}:{}", r.package, r.version), "release": r.release, "read": r.read, "packages": r.packages, "note": r.note }))
            .collect();
        set_repo_fields(
            cx.state,
            cx.team_id,
            repo,
            &[("base_images", attr_s(&Value::Array(summary).to_string()))],
        )
        .await;
    }
    for (i, change) in bi::apply_resolutions(findings, repo, reads, cx.cfg) {
        let (f, st) = &findings[i];
        save_finding(cx.state, cx.team_id, f, st).await;
        let line = match (&change, &f.fixed_version) {
            (Change::Fixed, _) => {
                "Fixed: the Dockerfile's base image no longer has the vulnerable package"
                    .to_string()
            }
            (Change::Updated, Some(to)) => format!("Fix found: pin {} to {to}", f.package),
            (Change::Updated, None) => continue,
        };
        add_event(cx.state, cx.team_id, &f.id, &line, None).await;
    }
}

/// The tick's per-finding work: the CISA list, EPSS, expired dismissals,
/// triage, tiers, pull request tracking, cards and fix runs.
async fn tend(
    cx: &Ctx<'_>,
    findings: &mut [(Finding, FindingState)],
    kev_map: &BTreeMap<String, String>,
    daily: bool,
    report: &mut TeamReport,
) {
    let now = Utc::now();
    // EPSS, once a day.
    let scores = if daily {
        let mut cves: Vec<String> = findings
            .iter()
            .filter(|(f, _)| f.status.is_open())
            .flat_map(|(f, _)| {
                f.advisory
                    .ids
                    .iter()
                    .filter(|i| i.starts_with("CVE-"))
                    .cloned()
            })
            .collect();
        cves.sort();
        cves.dedup();
        let (m, failed) = epss(cx.state, &cves).await;
        match failed {
            // Synced only when every batch was read: a failed read never
            // shows as a fresh sync.
            None => {
                set_team_state(
                    cx.state,
                    cx.team_id,
                    &[("epss_synced_at", attr_s(&now.to_rfc3339()))],
                )
                .await;
                record_problem(
                    cx.state,
                    cx.team_id,
                    sec::STATE_SK,
                    Check::Epss,
                    Outcome::Pass,
                )
                .await;
            }
            Some(why) => {
                warn!(team_id = cx.team_id, error = %why, "security: EPSS couldn't be read");
                record_problem(
                    cx.state,
                    cx.team_id,
                    sec::STATE_SK,
                    Check::Epss,
                    Outcome::Fail(
                        Kind::EpssReadFailed,
                        format!("FIRST's scores couldn't be read ({why}); tried again tomorrow."),
                    ),
                )
                .await;
            }
        }
        m
    } else {
        HashMap::new()
    };
    // The tier a finding's first card posted at, for cards posted before
    // it was kept: the tier it had then (read before triage moves it).
    for (f, st) in findings.iter_mut() {
        if st.carded_tier.is_none() && st.cards.iter().any(|c| c == "new") && f.tier.is_some() {
            st.carded_tier = f.tier;
            save_finding(cx.state, cx.team_id, f, st).await;
        }
    }
    // Triage by group: the code read once per repository, at most one model
    // call per package version, the most urgent first.
    report.triaged += triage_tick(cx, findings).await;
    for (f, st) in findings.iter_mut() {
        let before = (f.clone(), st.clone());
        let was_kev = f.advisory.kev;
        // An empty list is a failed read, never "nothing is exploited".
        if !kev_map.is_empty() {
            sec::apply_kev(f, kev_map);
        }
        if f.advisory.kev && !was_kev && f.status.is_open() {
            add_event(
                cx.state,
                cx.team_id,
                &f.id,
                "Added to CISA's list of exploited vulnerabilities",
                None,
            )
            .await;
        }
        if let Some((e, p)) = f
            .advisory
            .ids
            .iter()
            .find_map(|i| scores.get(&i.to_ascii_uppercase()))
        {
            let jumped = f.advisory.epss.is_none_or(|old| *e >= 0.1 && old < 0.1);
            f.advisory.epss = Some(*e);
            f.advisory.epss_percentile = Some(*p);
            if jumped && *e >= 0.1 && f.status.is_open() {
                add_event(
                    cx.state,
                    cx.team_id,
                    &f.id,
                    &format!("EPSS rose to {e:.2}"),
                    None,
                )
                .await;
            }
        }
        // A dismissal with an end date ends.
        if f.status == Status::Dismissed {
            let ended = f
                .dismissed
                .as_ref()
                .and_then(|d| d.until.as_deref())
                .and_then(|u| chrono::NaiveDate::parse_from_str(u.get(..10)?, "%Y-%m-%d").ok())
                .is_some_and(|d| d < now.date_naive());
            if ended {
                f.status = Status::Open;
                f.dismissed = None;
                add_event(
                    cx.state,
                    cx.team_id,
                    &f.id,
                    "Dismissal ended: open again",
                    None,
                )
                .await;
            }
        }
        sec::retier(f, st, cx.cfg);
        if (f.clone(), st.clone()) != before {
            f.updated_at = now.to_rfc3339();
            save_finding(cx.state, cx.team_id, f, st).await;
        }
    }
    track_prs(cx, findings).await;
    split_failed_majors(cx, findings).await;
    // Fixes a person asked for start whatever the dry run says.
    report.runs += start_requested(cx, findings).await;
    if cx.cfg.acts() {
        report.cards += cards(cx, findings).await;
        report.runs += start_fixes(cx, findings).await;
    }
}

fn verdict_word(v: Verdict) -> &'static str {
    match v {
        Verdict::Reachable => "reachable",
        Verdict::NotReachable => "present, not reachable",
        Verdict::NotLoaded => "not loaded",
        Verdict::Unknown => "unknown",
        Verdict::Pending => "pending",
    }
}

/// Open pull requests: their CI, its cards and its lines in the
/// description. A pull request that merged or closed, and a run that ended
/// without one, are `release`'s (it runs first, on its own clock); one that
/// merges or closes between the two is settled here by the same rules.
async fn track_prs(cx: &Ctx<'_>, findings: &mut [(Finding, FindingState)]) {
    let now = Utc::now();
    let mut seen: HashMap<(String, u64), (String, Option<String>)> = HashMap::new();
    let mut refreshed: HashSet<((String, u64), Option<String>)> = HashSet::new();
    let channels = if cx.cfg.acts() {
        Some(team_channels(cx.state, cx.team_id).await)
    } else {
        None
    };
    let io = release::Live::new(cx, release::TEAM_BUDGET_SECS);
    let mut facts: HashMap<(String, u64), common::security_release::PrNow> = HashMap::new();
    for (f, st) in findings.iter_mut() {
        if cx.out_of_time() {
            return;
        }
        let Some(pr) =
            f.pr.clone()
                .filter(|p| p.state == "open" && f.status == Status::PrOpen)
        else {
            continue;
        };
        let key = (f.repo.to_ascii_lowercase(), pr.number);
        let (state_now, ci) = match seen.get(&key) {
            Some(v) => v.clone(),
            None => {
                let v = pr_state(&cx.gh, &f.repo, pr.number).await;
                seen.insert(key.clone(), v.clone());
                v
            }
        };
        let before = f.clone();
        let mut p = pr.clone();
        p.ci = ci.clone().or(p.ci);
        match state_now.as_str() {
            "merged" | "closed" => {
                use release::ReleaseIo;
                let fact = match facts.get(&key) {
                    Some(v) => v.clone(),
                    None => {
                        let v = io.pr_now(&f.repo, pr.number, true).await;
                        facts.insert(key.clone(), v.clone());
                        v
                    }
                };
                if let Some(change) = common::security_release::settle_pr(f, st, &fact, now) {
                    save_finding(cx.state, cx.team_id, f, st).await;
                    add_event(
                        cx.state,
                        cx.team_id,
                        &f.id,
                        &change.text,
                        change.link.as_deref(),
                    )
                    .await;
                }
                p = f.pr.clone().unwrap_or(p);
            }
            _ => {
                if p.ci != pr.ci && p.ci.as_deref() == Some("passing") {
                    add_event(
                        cx.state,
                        cx.team_id,
                        &f.id,
                        &format!("CI passed on PR #{}", p.number),
                        Some(&p.url),
                    )
                    .await;
                }
                if p.ci != pr.ci && p.ci.as_deref() == Some("failing") {
                    add_event(
                        cx.state,
                        cx.team_id,
                        &f.id,
                        &format!("CI fails on PR #{}", p.number),
                        Some(&p.url),
                    )
                    .await;
                }
                // CI ended one way or the other: the description says so,
                // once per pull request and result.
                if p.ci != pr.ci
                    && matches!(p.ci.as_deref(), Some("passing" | "failing"))
                    && !p.adopted
                    && refreshed.insert((key.clone(), p.ci.clone()))
                {
                    if let Some(ticket) = st.ticket.clone() {
                        refresh_ci_sections(cx, &f.repo, p.number, &ticket).await;
                    }
                }
            }
        }
        if p.state == "open" {
            f.pr = Some(p.clone());
            if *f != before {
                f.updated_at = now.to_rfc3339();
                save_finding(cx.state, cx.team_id, f, st).await;
            }
        }
        // One card per pull request and stage, when its findings post.
        if let Some(ch) = &channels {
            if st.cards.iter().any(|c| c == "new") {
                let stage = match (p.state.as_str(), p.ci.as_deref()) {
                    ("merged", _) => Some(Stage::Merged),
                    ("open", Some("passing")) => Some(Stage::CiPassing),
                    _ => None,
                };
                if let Some(stage) = stage {
                    let thing = format!("PR#{}#{}", key.0, key.1);
                    post_card(cx.state, cx.team_id, ch, &thing, stage, f).await;
                }
            }
        }
    }
}

/// A pull request's state (`open`, `merged`, `closed`) and its CI.
async fn pr_state(gh: &GitHubClient, repo: &str, number: u64) -> (String, Option<String>) {
    let Some((o, n)) = repo.split_once('/') else {
        return ("open".into(), None);
    };
    let Ok(pr) = gh.get_pull_request(o, n, number).await else {
        return ("open".into(), None);
    };
    if pr["merged"].as_bool() == Some(true) || pr["merged_at"].is_string() {
        return ("merged".into(), None);
    }
    if pr["state"].as_str() == Some("closed") {
        return ("closed".into(), None);
    }
    let sha = pr["head"]["sha"].as_str().unwrap_or("");
    let ci = match gh.list_ci_for_ref(o, n, sha).await {
        Ok(v) => {
            let runs = v["check_runs"].as_array().cloned().unwrap_or_default();
            if runs.is_empty() {
                None
            } else if runs.iter().any(|r| {
                matches!(
                    r["conclusion"].as_str(),
                    Some("failure" | "timed_out" | "cancelled" | "action_required")
                )
            }) {
                Some("failing".to_string())
            } else if runs
                .iter()
                .all(|r| r["status"].as_str() == Some("completed"))
            {
                Some("passing".to_string())
            } else {
                Some("pending".to_string())
            }
        }
        Err(_) => None,
    };
    ("open".into(), ci)
}

async fn ticket_branch(state: &WorkerState, team_id: &str, ticket: &str) -> Option<String> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sec::ticket_sk(ticket)))
        .send()
        .await
        .ok()?;
    out.item().map(|i| s(i, "branch")).filter(|b| !b.is_empty())
}

/// Cards posted today, from the day's counter; `add` adds to it.
pub(crate) async fn cards_today(state: &WorkerState, team_id: &str, add: u32) -> u32 {
    let day = sec::limit_day(Utc::now());
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("SECCARDS#{day}")))
        .update_expression("ADD cards :n SET #ttl = if_not_exists(#ttl, :ttl)")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":n", attr_n(add))
        .expression_attribute_values(":ttl", attr_n(Utc::now().timestamp() + 3 * 86_400))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
        .send()
        .await;
    r.ok()
        .and_then(|o| o.attributes().and_then(|a| a.get("cards").cloned()))
        .and_then(|v| v.as_n().ok().and_then(|n| n.parse().ok()))
        // Can't be read: as if the day's cards are used.
        .unwrap_or(u32::MAX)
}

/// New cards and escalations, at most `max_cards_per_day` a day; the rest
/// go into one digest card a day.
async fn cards(cx: &Ctx<'_>, findings: &mut [(Finding, FindingState)]) -> usize {
    let channels = team_channels(cx.state, cx.team_id).await;
    if channels.is_empty() {
        return 0;
    }
    let now = Utc::now();
    let mut due: Vec<(usize, Stage)> = vec![];
    for (i, (f, st)) in findings.iter().enumerate() {
        if !f.status.is_open()
            || !sec::posts(f, cx.cfg)
            || !cx.cfg.repos.includes(&f.repo)
            || cx.cfg.ignored(f, now).is_some()
            || f.verdict == Verdict::Pending
        {
            continue;
        }
        let stage = if !st.cards.iter().any(|c| c == "new") {
            Some(Stage::New)
        } else {
            sec::escalation(f, st, cx.cfg, now)
        };
        if let Some(stage) = stage {
            due.push((i, stage));
        }
    }
    if due.is_empty() {
        return 0;
    }
    let posted_today = cards_today(cx.state, cx.team_id, 0).await;
    let refs: Vec<&Finding> = due.iter().map(|(i, _)| &findings[*i].0).collect();
    let (now_ids, later): (Vec<String>, Vec<String>) = {
        let (a, b) = sec::card_budget(refs, posted_today, cx.cfg.max_cards_per_day);
        (
            a.iter().map(|f| f.id.clone()).collect(),
            b.iter().map(|f| f.id.clone()).collect(),
        )
    };
    let mut posted = 0;
    for (i, stage) in due {
        if !now_ids.contains(&findings[i].0.id) {
            continue;
        }
        let (f, st) = &mut findings[i];
        if post_card(cx.state, cx.team_id, &channels, &f.id, stage, f).await {
            cards_today(cx.state, cx.team_id, 1).await;
            st.cards.push(stage.key().to_string());
            if stage == Stage::New {
                st.carded_tier = f.tier;
            }
            posted += 1;
            add_event(
                cx.state,
                cx.team_id,
                &f.id,
                &format!("Card posted ({})", stage_words(stage)),
                None,
            )
            .await;
            save_finding(cx.state, cx.team_id, f, st).await;
        }
    }
    if !later.is_empty() {
        let day = sec::limit_day(now);
        if claim(
            cx.state,
            cx.team_id,
            &format!("SECDIGEST#{day}"),
            2 * 86_400,
        )
        .await
        {
            let waiting: Vec<&Finding> = findings
                .iter()
                .filter(|(f, _)| later.contains(&f.id))
                .map(|(f, _)| f)
                .collect();
            let url = format!(
                "{}/security",
                super::skills::app_url(&cx.state.config.stage)
            );
            let msg = sec::digest_card(&waiting, &url, cx.cfg.max_cards_per_day);
            let sent = common::notify::deliver_recorded(
                &common::notify::Http(&cx.state.http),
                &channels,
                &msg,
                "security",
                &cx.state.dynamo,
                &cx.state.config.settings_table_name,
                cx.team_id,
            )
            .await;
            if sent.any_ok() {
                posted += 1;
            } else {
                release(cx.state, cx.team_id, &format!("SECDIGEST#{day}")).await;
            }
        }
    }
    posted
}

fn stage_words(stage: Stage) -> &'static str {
    match stage {
        Stage::New => "new finding",
        Stage::PrOpen => "pull request opened",
        Stage::CiPassing => "CI passing",
        Stage::Merged => "merged",
        Stage::Half => "half the deadline gone",
        Stage::Deadline => "deadline reached",
        Stage::Withdrawn => "withdrawn",
        Stage::RaisedP0 => "now P0",
    }
}

fn slots<'a>(cx: &'a Ctx<'_>) -> sec::DynamoSlots<'a> {
    sec::DynamoSlots {
        dynamo: &cx.state.dynamo,
        settings_table: &cx.state.config.settings_table_name,
        analytics_table: &cx.state.config.analytics_table_name,
        team_id: cx.team_id,
    }
}

/// Fix runs a person asked for that a daily limit held: they start as room
/// opens, the most urgent first, dry run or not (the person asked).
async fn start_requested(cx: &Ctx<'_>, findings: &mut [(Finding, FindingState)]) -> usize {
    let now = Utc::now();
    let units = sec::requested_units(findings, cx.cfg, now);
    if units.is_empty() {
        return 0;
    }
    let day = sec::limit_day(now);
    let slots = slots(cx);
    let mut busy: HashSet<String> = findings
        .iter()
        .filter(|(f, _)| f.status == Status::Fixing)
        .map(|(f, _)| f.repo.to_ascii_lowercase())
        .collect();
    let mut started = 0;
    for unit in units {
        if cx.out_of_time() {
            break;
        }
        let members: Vec<usize> = unit
            .finding_ids
            .iter()
            .filter_map(|id| findings.iter().position(|(f, _)| &f.id == id))
            .collect();
        let repo_busy = busy.contains(&unit.repo.to_ascii_lowercase());
        if let sec::Admit::Wait(why) = sec::admit_run(&slots, cx.cfg, &day, repo_busy).await {
            hold(cx, findings, &members, &why).await;
            continue;
        }
        if !claim(
            cx.state,
            cx.team_id,
            &format!("SEC_RUN#{}", unit.ticket),
            3600,
        )
        .await
        {
            slots.give_back(&day).await;
            continue;
        }
        let one_pr = members
            .iter()
            .any(|i| findings[*i].1.requested_as == sec::Combine::OnePr);
        match deliver(cx, &unit, findings, &members, one_pr).await {
            Ok(()) => {
                started += 1;
                busy.insert(unit.repo.to_ascii_lowercase());
                for i in &members {
                    findings[*i].1.requested_by = None;
                }
            }
            Err(e) => {
                warn!(team_id = cx.team_id, ticket = %unit.ticket, error = %e, "security: could not start the requested fix run");
                slots.give_back(&day).await;
                release(cx.state, cx.team_id, &format!("SEC_RUN#{}", unit.ticket)).await;
            }
        }
    }
    started
}

/// The `retry` event: the fix unit behind `ticket` is asked for again by
/// `by`, from its findings as they are now. First they are brought in line
/// with GitHub (the closed pull request releases them), then the ones a new
/// pull request can still fix carry the person's request, combined as the
/// unit was: `start_requested` starts it with room under the day's limits,
/// dry run or not, and the hold a closed pull request left does not apply
/// to a person's request. Returns how many findings were asked for.
async fn retry_asked(
    cx: &Ctx<'_>,
    findings: &mut [(Finding, FindingState)],
    ticket: &str,
    by: &str,
) -> usize {
    let Some(unit) = load_unit(cx.state, cx.team_id, ticket).await else {
        warn!(
            team_id = cx.team_id,
            ticket, "security: retry of a fix run whose plan is gone"
        );
        return 0;
    };
    let scope = release::Scope::Ids(unit.finding_ids.clone());
    release::run(cx, findings, &scope, release::TEAM_BUDGET_SECS).await;
    let asked = mark_retry(findings, &unit, by, Utc::now());
    for id in &asked {
        if let Some((f, st)) = findings.iter().find(|(f, _)| &f.id == id) {
            save_finding(cx.state, cx.team_id, f, st).await;
            add_event(
                cx.state,
                cx.team_id,
                &f.id,
                &format!("A new fix pull request was asked for by {by} (Retry on the fix run)"),
                None,
            )
            .await;
        }
    }
    info!(
        team_id = cx.team_id,
        ticket,
        by,
        findings = asked.len(),
        "security: fix run retried as a new request"
    );
    asked.len()
}

/// Pure: marks the unit's findings a retry asks for again (see
/// `security_release::retry_ids`) with who asked and how they combine, one
/// pull request for a bundle and one per package otherwise. Returns their
/// ids; empty when the unit still has an open pull request or a run.
pub(crate) fn mark_retry(
    findings: &mut [(Finding, FindingState)],
    unit: &FixUnit,
    by: &str,
    now: chrono::DateTime<Utc>,
) -> Vec<String> {
    let members: Vec<(Finding, FindingState)> = findings
        .iter()
        .filter(|(f, _)| unit.finding_ids.contains(&f.id))
        .cloned()
        .collect();
    let Some(ids) = common::security_release::retry_ids(&members) else {
        return vec![];
    };
    let combine = if unit.bundle {
        sec::Combine::OnePr
    } else {
        sec::Combine::PerPackage
    };
    for (f, st) in findings.iter_mut().filter(|(f, _)| ids.contains(&f.id)) {
        st.requested_by = Some(by.to_string());
        st.requested_as = combine;
        f.waiting = None;
        f.updated_at = now.to_rfc3339();
    }
    ids
}

/// The `fix` event: the gateway took the run slots for these findings when a
/// person asked; start their runs now.
async fn start_asked(
    cx: &Ctx<'_>,
    findings: &mut [(Finding, FindingState)],
    ids: &[String],
    by: &str,
    combine: sec::Combine,
) -> usize {
    let (units, _) = sec::manual_units_as(findings, ids, cx.cfg, Utc::now(), combine);
    let mut started = 0;
    for unit in units {
        let members: Vec<usize> = unit
            .finding_ids
            .iter()
            .filter_map(|id| findings.iter().position(|(f, _)| &f.id == id))
            .collect();
        if !claim(
            cx.state,
            cx.team_id,
            &format!("SEC_RUN#{}", unit.ticket),
            3600,
        )
        .await
        {
            continue;
        }
        match deliver(
            cx,
            &unit,
            findings,
            &members,
            combine == sec::Combine::OnePr,
        )
        .await
        {
            Ok(()) => started += 1,
            Err(e) => {
                warn!(team_id = cx.team_id, ticket = %unit.ticket, by, error = %e, "security: could not start the asked fix run");
                release(cx.state, cx.team_id, &format!("SEC_RUN#{}", unit.ticket)).await;
                // Its slot is given back and the request waits for the next tick.
                sec::DailySlots::give_back(&slots(cx), &sec::limit_day(Utc::now())).await;
                for i in &members {
                    let (f, st) = &mut findings[*i];
                    st.requested_by = Some(by.to_string());
                    st.requested_as = combine;
                    f.waiting = Some(
                        "Waiting: the fix run couldn't start; it is tried again on the next check."
                            .into(),
                    );
                    save_finding(cx.state, cx.team_id, f, st).await;
                }
            }
        }
    }
    started
}

/// Fix runs (or adoptions) for the units due now, within the day's limits:
/// at most `max_fix_runs_per_day` runs and `max_fix_tokens_per_day` tokens,
/// one run in progress per repository, the most urgent first. Adopting a
/// bot's pull request or opening an issue costs no run.
async fn start_fixes(cx: &Ctx<'_>, findings: &mut [(Finding, FindingState)]) -> usize {
    let now = Utc::now();
    let day = sec::limit_day(now);
    let slots = slots(cx);
    let mut units = sec::plan_units(findings, cx.cfg, now);
    sec::order_units(&mut units, findings);
    // The findings the rules hold out (a major they don't need yet, a bump a
    // run found breaking) say why on the dashboard.
    for (id, why) in sec::deferred_waits(findings, cx.cfg, now) {
        if let Some(i) = findings.iter().position(|(f, _)| f.id == id) {
            hold(cx, findings, &[i], &why).await;
        }
    }
    let mut busy: HashSet<String> = findings
        .iter()
        .filter(|(f, _)| f.status == Status::Fixing)
        .map(|(f, _)| f.repo.to_ascii_lowercase())
        .collect();
    let mut started = 0;
    let mut open_pulls: HashMap<String, Vec<Value>> = HashMap::new();
    for unit in units {
        if cx.out_of_time() || !sec::unit_due(&unit, cx.cfg) {
            continue;
        }
        let members: Vec<usize> = unit
            .finding_ids
            .iter()
            .filter_map(|id| findings.iter().position(|(f, _)| &f.id == id))
            .collect();
        // An open Dependabot or Renovate pull request for the package.
        let mut bot_pr = None;
        if cx.cfg.adopt_bot_prs && !unit.grouped && !unit.bundle {
            let pulls = match open_pulls.get(&unit.repo) {
                Some(p) => p.clone(),
                None => {
                    let (o, n) = unit.repo.split_once('/').unwrap_or(("", ""));
                    let p = cx
                        .gh
                        .list_open_pulls(o, n)
                        .await
                        .ok()
                        .and_then(|v| v.as_array().cloned())
                        .unwrap_or_default();
                    open_pulls.insert(unit.repo.clone(), p.clone());
                    p
                }
            };
            bot_pr = bot_pr_for(&pulls, &unit);
        }
        let major = unit.bumps.iter().any(|b| b.major);
        let kind = if bot_pr.is_some() {
            sec::RunKind::Adopt
        } else if major && !unit.bundle && cx.cfg.major_bumps == sec::MajorBumps::IssueOnly {
            sec::RunKind::IssueOnly
        } else {
            sec::RunKind::Run
        };
        if kind.counts() {
            if let Some(why) = super::check_budget_exceeded(cx.state, cx.team_id).await {
                info!(team_id = cx.team_id, ticket = %unit.ticket, why, "security: fix run not started (budget)");
                hold(cx, findings, &members, &format!("Waiting: {why}.")).await;
                continue;
            }
            let repo_busy = busy.contains(&unit.repo.to_ascii_lowercase());
            if let sec::Admit::Wait(why) = sec::admit_run(&slots, cx.cfg, &day, repo_busy).await {
                hold(cx, findings, &members, &why).await;
                continue;
            }
        }
        if unit.grouped {
            let sk = format!("SEC_BATCH#{}#{}", unit.repo.to_ascii_lowercase(), unit.key);
            if !claim(cx.state, cx.team_id, &sk, 8 * 86_400).await {
                if kind.counts() {
                    slots.give_back(&day).await;
                }
                continue;
            }
        }
        if !claim(
            cx.state,
            cx.team_id,
            &format!("SEC_RUN#{}", unit.ticket),
            3600,
        )
        .await
        {
            if kind.counts() {
                slots.give_back(&day).await;
            }
            continue;
        }
        match (kind, bot_pr) {
            (sec::RunKind::Adopt, Some(pr)) => {
                adopt(cx, &unit, &pr, findings, &members).await;
                started += 1;
            }
            (sec::RunKind::IssueOnly, _) => issue_only(cx, &unit, findings, &members).await,
            _ => match deliver(cx, &unit, findings, &members, false).await {
                Ok(()) => {
                    started += 1;
                    busy.insert(unit.repo.to_ascii_lowercase());
                }
                Err(e) => {
                    warn!(team_id = cx.team_id, ticket = %unit.ticket, error = %e, "security: could not start the fix run");
                    slots.give_back(&day).await;
                    release(cx.state, cx.team_id, &format!("SEC_RUN#{}", unit.ticket)).await;
                }
            },
        }
    }
    started
}

/// Starts a unit's fixes: a follow-up run on the repository's open bundle
/// pull request when there is one (one open security pull request per
/// repository), otherwise a new run. `one_pr`: a person asked for one pull
/// request, so any open CoderHelm security pull request of the repository
/// takes the fixes.
async fn deliver(
    cx: &Ctx<'_>,
    unit: &FixUnit,
    findings: &mut [(Finding, FindingState)],
    members: &[usize],
    one_pr: bool,
) -> Result<(), Error> {
    if let Some((pr, ticket)) = sec::open_bundle_pr(findings, unit, one_pr) {
        return join(cx, unit, &pr, &ticket, findings, members).await;
    }
    enqueue(cx, unit, findings, members).await
}

/// The ticket record's own fields: the stored plan and the run's words
/// under Change.
async fn ticket_item(state: &WorkerState, team_id: &str, ticket: &str) -> Option<Item> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sec::ticket_sk(ticket)))
        .send()
        .await
        .ok()?
        .item
}

async fn save_unit(
    state: &WorkerState,
    team_id: &str,
    ticket: &str,
    unit: &FixUnit,
) -> Result<(), Error> {
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sec::ticket_sk(ticket)))
        .update_expression("SET #u = :u, title = :t")
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_names("#u", "unit")
        .expression_attribute_values(":u", attr_s(&serde_json::to_string(unit)?))
        .expression_attribute_values(":t", attr_s(&unit.title))
        .send()
        .await?;
    Ok(())
}

/// Sends a follow-up round to the run that opened a security pull
/// request, the way a review on it is sent: the run's fix loop pushes to
/// the same branch. Its tokens count as the ticket's fix run.
async fn follow_up(
    cx: &Ctx<'_>,
    repo: &str,
    pr: u64,
    ticket: &str,
    ask: String,
) -> Result<(), Error> {
    if cx.state.config.feedback_queue_url.is_empty() {
        return Err("no feedback queue".into());
    }
    let (owner, name) = repo.split_once('/').ok_or("bad repo")?;
    let run_id = super::review_pr::lookup_run_by_pr(cx.state, cx.team_id, owner, name, pr)
        .await
        .ok_or("no run found for the open security pull request")?;
    let msg = WorkerMessage::Feedback(common::messages::FeedbackMessage {
        team_id: cx.team_id.to_string(),
        installation_id: cx.gh.installation_id(),
        run_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number: pr,
        review_id: 0,
        review_body: ask,
        comments: vec![],
        security_ticket: ticket.to_string(),
        ..Default::default()
    });
    cx.state
        .sqs
        .send_message()
        .queue_url(&cx.state.config.feedback_queue_url)
        .message_body(serde_json::to_string(&msg)?)
        .send()
        .await?;
    Ok(())
}

/// Writes an open bundle's title, description and labels again from its
/// plan. Best effort: the findings already point at it.
async fn refresh_pr(
    cx: &Ctx<'_>,
    unit: &FixUnit,
    ticket: &str,
    pr: u64,
    findings: &[(Finding, FindingState)],
) {
    let Some((o, n)) = unit.repo.split_once('/') else {
        return;
    };
    let item = ticket_item(cx.state, cx.team_id, ticket).await;
    let change = item.as_ref().map(|i| s(i, "change")).unwrap_or_default();
    let pairs: Vec<(&Finding, &FindingState)> = findings
        .iter()
        .filter(|(f, _)| unit.finding_ids.contains(&f.id))
        .map(|(f, st)| (f, st))
        .collect();
    let check = what_to_check(
        &cx.gh,
        unit,
        item.as_ref()
            .map(|i| s(i, "notes"))
            .unwrap_or_default()
            .as_str(),
        Some(pr),
    )
    .await;
    let links = body_links(cx.state, item.as_ref());
    // What the branch resolves now, with the fixes that joined or left.
    let base = cx.gh.get_default_branch(o, n).await.unwrap_or_default();
    let head = head_facts(&cx.gh, unit, &base, &unit.branch).await;
    let body = sec::pr_body_at_head(unit, &pairs, &change, &check, &links, &head);
    if let Err(e) = crate::passes::description::save(&cx.gh, o, n, pr, &body).await {
        warn!(repo = %unit.repo, pr, error = %e, "security: could not write the bundle's description");
    }
    if let Err(e) = cx.gh.update_pr_title(o, n, pr, &unit.title).await {
        warn!(repo = %unit.repo, pr, error = %e, "security: could not write the bundle's title");
    }
    crate::passes::description::reconcile(cx.state, &cx.gh, cx.team_id, o, n, pr, ticket).await;
    relabel(cx, o, n, pr, unit).await;
}

/// Labels an open pull request with the unit's labels, first taking off a
/// reachability label an earlier verdict left (GitHub only adds labels), so
/// the PR never says `reachability:unknown` beside `not-reachable`.
async fn relabel(cx: &Ctx<'_>, o: &str, n: &str, pr: u64, unit: &FixUnit) {
    for gone in sec::stale_reachability_labels(&unit.labels) {
        // A label the PR does not carry is a 404: nothing to take off.
        let _ = cx.gh.remove_label(o, n, pr, gone).await;
    }
    if let Err(e) = cx.gh.add_labels(o, n, pr, &unit.labels).await {
        warn!(repo = %unit.repo, pr, error = %e, "security: could not label the pull request");
    }
}

/// Adds a unit's fixes to the repository's open bundle pull request: a
/// follow-up run on its ticket pushes them to its branch, and its title,
/// description and labels count them.
async fn join(
    cx: &Ctx<'_>,
    add: &FixUnit,
    pr: &sec::PrRef,
    ticket: &str,
    findings: &mut [(Finding, FindingState)],
    members: &[usize],
) -> Result<(), Error> {
    let open = load_unit(cx.state, cx.team_id, ticket)
        .await
        .ok_or("the open bundle's plan is gone")?;
    let merged = sec::join_unit(&open, add, findings, cx.cfg);
    let member_refs: Vec<&Finding> = members.iter().map(|i| &findings[*i].0).collect();
    let (o, n) = add.repo.split_once('/').ok_or("bad repo")?;
    let base = cx.gh.get_default_branch(o, n).await?;
    let deps = load_deps(cx.state, cx.team_id, &add.repo)
        .await
        .map(|d| d.0)
        .unwrap_or_default();
    let member_pairs: Vec<(&Finding, &FindingState)> = members
        .iter()
        .map(|i| (&findings[*i].0, &findings[*i].1))
        .collect();
    let notes =
        super::upgrade_notes::read(cx.state, &cx.gh, add, &base, &member_pairs, &deps).await;
    let ask = sec::join_note_with(add, &member_refs, cx.cfg, &notes);
    follow_up(cx, &add.repo, pr.number, ticket, ask).await?;
    save_unit(cx.state, cx.team_id, ticket, &merged).await?;
    let kept = ticket_item(cx.state, cx.team_id, ticket)
        .await
        .map(|i| s(&i, "notes"))
        .unwrap_or_default();
    save_notes(
        cx.state,
        cx.team_id,
        ticket,
        &super::upgrade_notes::merged(&kept, &notes),
    )
    .await;
    let now = Utc::now().to_rfc3339();
    for i in members {
        let (f, st) = &mut findings[*i];
        f.status = Status::PrOpen;
        f.pr = Some(sec::PrRef {
            ci: Some("pending".into()),
            ..pr.clone()
        });
        f.waiting = None;
        st.requested_by = None;
        st.ticket = Some(ticket.to_string());
        st.bundled = true;
        st.pr_opened_at.get_or_insert_with(|| now.clone());
        f.updated_at = now.clone();
        save_finding(cx.state, cx.team_id, f, st).await;
        add_event(
            cx.state,
            cx.team_id,
            &f.id,
            &format!(
                "Added to PR #{} with the repository's other security fixes",
                pr.number
            ),
            Some(&pr.url),
        )
        .await;
    }
    refresh_pr(cx, &merged, ticket, pr.number, findings).await;
    info!(team_id = cx.team_id, ticket, repo = %add.repo, pr = pr.number, findings = members.len(), "security: fixes joined the open bundle");
    Ok(())
}

/// Whether the run's CI-fix loop gave up on its pull request (it handed
/// it to a person).
async fn handed_off(state: &WorkerState, team_id: &str, run_id: &str) -> bool {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("handed_off_at")
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .is_some_and(|i| i.contains_key("handed_off_at"))
}

/// A bundle whose CI the fix loop couldn't get green, with major version
/// bumps in it: the major bumps leave it (a follow-up run reverts them on
/// its branch) and get pull requests of their own on the next tick. Once
/// per pull request. A bundle that passes keeps its major bumps.
async fn split_failed_majors(cx: &Ctx<'_>, findings: &mut [(Finding, FindingState)]) {
    let mut seen = HashSet::new();
    let failing: Vec<(String, sec::PrRef, String)> = findings
        .iter()
        .filter(|(f, st)| {
            st.bundled
                && f.status == Status::PrOpen
                && f.pr.as_ref().is_some_and(|p| {
                    p.state == "open" && !p.adopted && p.ci.as_deref() == Some("failing")
                })
        })
        .filter_map(|(f, st)| Some((f.repo.clone(), f.pr.clone()?, st.ticket.clone()?)))
        .filter(|(r, p, _)| seen.insert((r.to_ascii_lowercase(), p.number)))
        .collect();
    for (repo, pr, ticket) in failing {
        if cx.out_of_time() {
            return;
        }
        let Some(unit) = load_unit(cx.state, cx.team_id, &ticket).await else {
            continue;
        };
        let gone = sec::split_out(&unit, findings);
        if gone.is_empty() {
            continue;
        }
        let Some((o, n)) = repo.split_once('/') else {
            continue;
        };
        let Some(run_id) =
            super::review_pr::lookup_run_by_pr(cx.state, cx.team_id, o, n, pr.number).await
        else {
            continue;
        };
        if !handed_off(cx.state, cx.team_id, &run_id).await {
            continue;
        }
        let claim_sk = format!("SEC_SPLIT#{}#{}", repo.to_ascii_lowercase(), pr.number);
        if !claim(cx.state, cx.team_id, &claim_sk, sec::CARD_CLAIM_SECS).await {
            continue;
        }
        let majors: Vec<&sec::Bump> = unit.bumps.iter().filter(|b| b.major).collect();
        let left = sec::without(&unit, &gone, findings, cx.cfg);
        if let Err(e) = follow_up(cx, &repo, pr.number, &ticket, sec::split_note(&majors)).await {
            warn!(team_id = cx.team_id, %repo, pr = pr.number, error = %e, "security: could not take the major bumps out of the bundle");
            release(cx.state, cx.team_id, &claim_sk).await;
            continue;
        }
        if let Err(e) = save_unit(cx.state, cx.team_id, &ticket, &left).await {
            warn!(team_id = cx.team_id, %repo, error = %e, "security: could not save the bundle without its major bumps");
        }
        let now = Utc::now();
        for (f, st) in findings.iter_mut().filter(|(f, _)| gone.contains(&f.id)) {
            f.status = Status::Open;
            f.pr = None;
            f.waiting = None;
            st.ticket = None;
            st.bundled = false;
            st.split = true;
            f.updated_at = now.to_rfc3339();
            save_finding(cx.state, cx.team_id, f, st).await;
            add_event(
                cx.state,
                cx.team_id,
                &f.id,
                &format!(
                    "Taken out of PR #{}: its major version bump failed CI there. It gets a pull request of its own.",
                    pr.number
                ),
                Some(&pr.url),
            )
            .await;
        }
        refresh_pr(cx, &left, &ticket, pr.number, findings).await;
        info!(team_id = cx.team_id, %repo, pr = pr.number, split = gone.len(), "security: major bumps split out of a failing bundle");
    }
}

/// Notes on a unit's findings why its fix hasn't started.
async fn hold(
    cx: &Ctx<'_>,
    findings: &mut [(Finding, FindingState)],
    members: &[usize],
    why: &str,
) {
    for i in members {
        let (f, st) = &mut findings[*i];
        if f.waiting.as_deref() != Some(why) {
            f.waiting = Some(why.to_string());
            save_finding(cx.state, cx.team_id, f, st).await;
        }
    }
}

/// A bot's pull request that already bumps the unit's package.
fn bot_pr_for(pulls: &[Value], unit: &FixUnit) -> Option<Value> {
    let pkg = unit.bumps.first()?.package.to_ascii_lowercase();
    pulls
        .iter()
        .find(|p| {
            let head = p["head"]["ref"].as_str().unwrap_or("");
            let title = p["title"].as_str().unwrap_or("").to_ascii_lowercase();
            (head.starts_with("dependabot/") || head.starts_with("renovate/"))
                && (title.contains(&format!(" {pkg} ")) || head.to_ascii_lowercase().contains(&pkg))
        })
        .cloned()
}

async fn adopt(
    cx: &Ctx<'_>,
    unit: &FixUnit,
    pr: &Value,
    findings: &mut [(Finding, FindingState)],
    members: &[usize],
) {
    let number = pr["number"].as_u64().unwrap_or(0);
    let url = pr["html_url"].as_str().unwrap_or("").to_string();
    let Some((o, n)) = unit.repo.split_once('/') else {
        return;
    };
    relabel(cx, o, n, number, unit).await;
    if claim(
        cx.state,
        cx.team_id,
        &format!("SEC_ADOPT_NOTE#{}#{number}", unit.repo.to_ascii_lowercase()),
        sec::CARD_CLAIM_SECS,
    )
    .await
    {
        let pairs: Vec<(&Finding, &FindingState)> = members
            .iter()
            .map(|i| (&findings[*i].0, &findings[*i].1))
            .collect();
        let mut body = String::from("CoderHelm is tracking this pull request as the fix for:\n\n");
        for (f, _) in &pairs {
            body.push_str(&format!(
                "- {} in `{}` {}: {}{}\n",
                f.advisory.primary,
                f.package,
                f.version,
                verdict_word(f.verdict),
                f.evidence
                    .as_ref()
                    .map(|e| format!(". {}", e.summary))
                    .unwrap_or_default()
            ));
            if let Some(e) = &f.evidence {
                for l in e.lines.iter().take(5) {
                    body.push_str(&format!("  - `{}:{}`\n", l.file, l.line));
                }
            }
        }
        body.push_str(&format!("\n{}\n\n{}", sec::ATTRIBUTION, sec::MARKER));
        let _ = cx.gh.create_issue_comment(o, n, number, &body).await;
    }
    for i in members {
        let (f, st) = &mut findings[*i];
        f.status = Status::PrOpen;
        f.pr = Some(sec::PrRef {
            number,
            url: url.clone(),
            ci: Some("pending".into()),
            state: "open".into(),
            adopted: true,
            ..Default::default()
        });
        f.waiting = None;
        st.pr_opened_at
            .get_or_insert_with(|| Utc::now().to_rfc3339());
        f.updated_at = Utc::now().to_rfc3339();
        save_finding(cx.state, cx.team_id, f, st).await;
        add_event(
            cx.state,
            cx.team_id,
            &f.id,
            &format!("Adopted the existing pull request #{number}"),
            Some(&url),
        )
        .await;
    }
}

async fn issue_only(
    cx: &Ctx<'_>,
    unit: &FixUnit,
    findings: &mut [(Finding, FindingState)],
    members: &[usize],
) {
    let Some((o, n)) = unit.repo.split_once('/') else {
        return;
    };
    let pairs: Vec<(&Finding, &FindingState)> = members
        .iter()
        .map(|i| (&findings[*i].0, &findings[*i].1))
        .collect();
    let title = unit
        .title
        .replacen("fix(deps): bump", "Security: upgrade", 1);
    let body = sec::pr_body(unit, &pairs, "A major version is needed. The team chose to track major upgrades as issues instead of pull requests.");
    let issue = cx.gh.create_issue(o, n, &title, &body).await;
    for i in members {
        let (f, st) = &mut findings[*i];
        st.hold_until = Some((Utc::now() + Duration::days(30)).to_rfc3339());
        f.updated_at = Utc::now().to_rfc3339();
        save_finding(cx.state, cx.team_id, f, st).await;
        if let Ok((num, url)) = &issue {
            add_event(
                cx.state,
                cx.team_id,
                &f.id,
                &format!("Issue #{num} opened (a major version is needed)"),
                Some(url),
            )
            .await;
        }
    }
}

async fn enqueue(
    cx: &Ctx<'_>,
    unit: &FixUnit,
    findings: &mut [(Finding, FindingState)],
    members: &[usize],
) -> Result<(), Error> {
    if cx.state.config.ticket_queue_url.is_empty() {
        return Err("no ticket queue".into());
    }
    let (owner, name) = unit.repo.split_once('/').ok_or("bad repo")?;
    let base = cx.gh.get_default_branch(owner, name).await?;
    let member_refs: Vec<&Finding> = members.iter().map(|i| &findings[*i].0).collect();
    // What each bump changes, read before the run: the coder starts from
    // the breaking changes and their call sites, and the pull request's
    // "What to check" is written from the same notes.
    let deps = load_deps(cx.state, cx.team_id, &unit.repo)
        .await
        .map(|d| d.0)
        .unwrap_or_default();
    let member_pairs: Vec<(&Finding, &FindingState)> = members
        .iter()
        .map(|i| (&findings[*i].0, &findings[*i].1))
        .collect();
    let notes =
        super::upgrade_notes::read(cx.state, &cx.gh, unit, &base, &member_pairs, &deps).await;
    let body = sec::ticket_body_with(unit, &member_refs, cx.cfg, &notes);
    let lead = member_refs
        .iter()
        .min_by_key(|f| f.tier)
        .map(|f| f.id.clone())
        .unwrap_or_default();
    cx.state
        .dynamo
        .put_item()
        .table_name(&cx.state.config.settings_table_name)
        .item("pk", attr_s(cx.team_id))
        .item("sk", attr_s(&sec::ticket_sk(&unit.ticket)))
        .item("unit", attr_s(&serde_json::to_string(unit)?))
        .item("repo", attr_s(&unit.repo))
        .item("branch", attr_s(&unit.branch))
        .item("title", attr_s(&unit.title))
        .item("lead_finding", attr_s(&lead))
        .item("notes", attr_s(&serde_json::to_string(&notes)?))
        .item("ttl", attr_n(Utc::now().timestamp() + 120 * 86_400))
        .send()
        .await?;
    let msg = WorkerMessage::Ticket(TicketMessage {
        team_id: cx.team_id.to_string(),
        installation_id: cx.gh.installation_id(),
        source: TicketSource::Alert,
        ticket_id: unit.ticket.clone(),
        title: unit.title.clone(),
        body,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        issue_number: 0,
        sender: "security".into(),
        requester: Some(common::requester::Requester::coderhelm(
            common::requester::SECURITY,
        )),
        base_branch: base,
        ..Default::default()
    });
    cx.state
        .sqs
        .send_message()
        .queue_url(&cx.state.config.ticket_queue_url)
        .message_body(serde_json::to_string(&msg)?)
        .send()
        .await?;
    for i in members {
        let (f, st) = &mut findings[*i];
        f.status = Status::Fixing;
        f.waiting = None;
        st.requested_by = None;
        st.bundled = unit.bundle;
        st.ticket = Some(unit.ticket.clone());
        st.claimed_at = Some(Utc::now().to_rfc3339());
        f.updated_at = Utc::now().to_rfc3339();
        save_finding(cx.state, cx.team_id, f, st).await;
        add_event(
            cx.state,
            cx.team_id,
            &f.id,
            &format!("Fix run started: {}", unit.title),
            None,
        )
        .await;
    }
    info!(team_id = cx.team_id, ticket = %unit.ticket, repo = %unit.repo, findings = members.len(), "security: fix run queued");
    Ok(())
}

/// Closes pull requests that existed only for withdrawn advisories, and
/// posts the withdrawn card where a card posted.
async fn close_withdrawn(cx: &Ctx<'_>, findings: &mut [(Finding, FindingState)]) {
    let to_close = sec::prs_to_close_on_withdraw(findings);
    for (repo, number) in to_close {
        let Some((o, n)) = repo.split_once('/') else {
            continue;
        };
        if !claim(
            cx.state,
            cx.team_id,
            &format!("SEC_WITHDRAW_CLOSE#{repo}#{number}"),
            sec::CARD_CLAIM_SECS,
        )
        .await
        {
            continue;
        }
        let note = format!("The advisory this pull request fixes was withdrawn, so CoderHelm closed it. The branch is kept.\n\n{}", sec::MARKER);
        let _ = cx.gh.create_issue_comment(o, n, number, &note).await;
        if let Err(e) = cx.gh.close_pull_request(o, n, number).await {
            warn!(repo, number, error = %e, "security: could not close the withdrawn advisory's pull request");
        }
        for (f, st) in findings.iter_mut() {
            if f.repo.eq_ignore_ascii_case(&repo)
                && f.pr.as_ref().is_some_and(|p| p.number == number)
            {
                if let Some(p) = &mut f.pr {
                    p.state = "closed".into();
                }
                save_finding(cx.state, cx.team_id, f, st).await;
                add_event(
                    cx.state,
                    cx.team_id,
                    &f.id,
                    &format!("PR #{number} closed: the advisory was withdrawn"),
                    None,
                )
                .await;
            }
        }
    }
    let channels = team_channels(cx.state, cx.team_id).await;
    for (f, st) in findings.iter_mut() {
        if f.status == Status::Withdrawn
            && st.cards.iter().any(|c| c == "new")
            && !st.cards.iter().any(|c| c == "withdrawn")
            && post_card(cx.state, cx.team_id, &channels, &f.id, Stage::Withdrawn, f).await
        {
            st.cards.push("withdrawn".into());
            save_finding(cx.state, cx.team_id, f, st).await;
        }
    }
}

async fn get_team_state(state: &WorkerState, team_id: &str) -> Option<Item> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sec::STATE_SK))
        .send()
        .await
        .ok()?
        .item()
        .cloned()
}

/// Dependabot only: reads a repository's alerts (when access was missing,
/// to see whether it works now) and applies them.
async fn dependabot_sync(
    cx: &Ctx<'_>,
    repo: &str,
    findings: &mut Vec<(Finding, FindingState)>,
) -> usize {
    let (alerts, access) = dependabot_alerts(&cx.gh, repo, 5).await;
    record_dependabot(cx, repo, &access).await;
    if alerts.is_empty() {
        return 0;
    }
    let deps = load_deps(cx.state, cx.team_id, repo)
        .await
        .map(|(d, _)| d)
        .unwrap_or_default();
    let obs: Vec<Observation> = alerts
        .iter()
        .filter_map(|a| sec::observation_from_alert(repo, a))
        .map(|mut o| {
            fill_version(&mut o, &deps);
            o
        })
        .collect();
    upsert(cx.state, cx.team_id, cx.cfg, findings, obs)
        .await
        .created
}

/// One team's tick. `kev_map`: the CISA list when the sweep still has time
/// for the team's sources, triage and fix runs; `None` when its time is
/// spent on the teams before, and only the release runs (it has a clock of
/// its own, so no team's stale findings wait on another team's work).
async fn team_tick(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    cfg: &SecurityConfig,
    kev_map: Option<&BTreeMap<String, String>>,
    started: Instant,
) -> Result<TeamReport, Error> {
    let cx = Ctx {
        state,
        team_id,
        cfg,
        gh: github(state, installation_id)?,
        started,
        osv: Default::default(),
    };
    let mut report = TeamReport::default();
    let mut findings = load_findings(state, team_id).await;
    // First, and whatever else the tick has time for: every finding in a
    // pull request or a run is checked against GitHub and the run's record.
    report.released = release::run(
        &cx,
        &mut findings,
        &release::Scope::All,
        release::TEAM_BUDGET_SECS,
    )
    .await
    .changed();
    let Some(kev_map) = kev_map else {
        return Ok(report);
    };
    let day = Utc::now().format("%Y%m%d").to_string();
    // Dependabot access is read again every sweep until it works, so a
    // permission accepted on GitHub shows within one sweep.
    let access_off = get_team_state(state, team_id)
        .await
        .map(|i| s(&i, "dependabot"))
        .is_some_and(|d| !d.is_empty() && d != "on");
    for repo in team_repos(state, team_id, cfg).await {
        if cx.out_of_time() {
            break;
        }
        if !claim(
            state,
            team_id,
            &format!("SECSYNC#{}#{day}", repo.to_ascii_lowercase()),
            2 * 86_400,
        )
        .await
        {
            if access_off {
                report.new_findings += dependabot_sync(&cx, &repo, &mut findings).await;
            }
            continue;
        }
        report.new_findings += sync_repo(&cx, &repo, &mut findings, true).await;
        report.repos_synced += 1;
    }
    // GitHub's advisory database, about hourly: the webhook's backstop.
    let hour = Utc::now().format("%Y%m%d%H").to_string();
    if !cx.out_of_time() && claim(state, team_id, &format!("SECADVPOLL#{hour}"), 2 * 3600).await {
        report.new_findings += poll_advisories(&cx, &mut findings).await;
    }
    let daily = claim(state, team_id, &format!("SECDAILY#{day}"), 2 * 86_400).await;
    tend(&cx, &mut findings, kev_map, daily, &mut report).await;
    if cfg.acts() {
        close_withdrawn(&cx, &mut findings).await;
    }
    Ok(report)
}

/// What one scheduled sweep did.
#[derive(Debug, Default)]
pub struct SweepReport {
    pub teams: usize,
    /// Findings brought in line with their pull request or run.
    pub released: usize,
    pub repos_synced: usize,
    pub new_findings: usize,
    pub triaged: usize,
    pub runs: usize,
    pub cards: usize,
}

/// An observation withdrawing `ghsa` from every finding that names it,
/// whatever its version now.
fn withdrawn_observations(findings: &[(Finding, FindingState)], ghsa: &str) -> Vec<Observation> {
    findings
        .iter()
        .filter(|(f, _)| f.advisory.ids.iter().any(|i| i.eq_ignore_ascii_case(ghsa)))
        .map(|(f, _)| Observation {
            repo: f.repo.clone(),
            manifest: f.manifest.clone(),
            ecosystem: f.ecosystem.clone(),
            package: f.package.clone(),
            version: f.version.clone(),
            advisory: sec::AdvisoryRecord {
                ids: vec![ghsa.to_string()],
                withdrawn: true,
                source: "github_advisory".into(),
                ..Default::default()
            },
            ..Default::default()
        })
        .collect()
}

/// What an advisory poll reads and writes: GitHub's advisory database, the
/// team's row (its checkpoint and status), and the findings each page's
/// matches become. A trait so the paging can be tested on a fake GitHub.
#[async_trait::async_trait]
trait AdvisoryPollIo: Send {
    /// The poll's fields on the team's row (empty strings when unset).
    async fn poll_fields(&self) -> HashMap<String, String>;
    async fn save_poll_fields(&self, fields: &[(&'static str, String)]);
    /// One `GET`: status, body and `Link` header.
    async fn page(&self, path: &str) -> Result<(u16, Value, Option<String>), String>;
    /// Matches one page against the dependency lists and applies it:
    /// (matched, created).
    async fn apply(&mut self, advisories: &[Value]) -> (usize, usize);
    fn out_of_time(&self) -> bool;
}

/// Fields of the team's row the advisory poll keeps.
const POLL_FIELDS: [&str; 6] = [
    "advisory_polled_at",
    "advisory_poll_from",
    "advisory_poll_after",
    "advisory_poll_run_at",
    "advisory_poll_caught_up",
    "advisory_poll_error",
];

/// What one advisory poll did.
#[derive(Debug, Default)]
struct AdvisoryPollReport {
    since: String,
    pages: usize,
    read: usize,
    matched: usize,
    created: usize,
    caught_up: bool,
    error: Option<String>,
}

/// One advisory poll: from the checkpoint (or the last poll that reached
/// the end), page by page in `updated_at` order. Each page is matched and
/// applied before the checkpoint moves past it, so a run stopped by its
/// page budget, its time budget or a failed request goes on from there next
/// time instead of starting over. Reaching the end records
/// `advisory_polled_at` (the run's start) and clears the checkpoint.
async fn run_advisory_poll(
    io: &mut dyn AdvisoryPollIo,
    now: chrono::DateTime<Utc>,
) -> AdvisoryPollReport {
    let f = io.poll_fields().await;
    let get = |k: &str| f.get(k).map(String::as_str).filter(|v| !v.is_empty());
    let start = sec::advisory_poll_start(
        get("advisory_polled_at"),
        get("advisory_poll_from"),
        get("advisory_poll_after"),
        now,
    );
    let budget = if start.behind {
        sec::ADVISORY_POLL_PAGES_BEHIND
    } else {
        sec::ADVISORY_POLL_PAGES
    };
    let mut report = AdvisoryPollReport {
        since: start.since.clone(),
        ..Default::default()
    };
    let mut after = start.after.clone();
    let mut saved_cursor = after.is_some();
    while report.pages < budget {
        if report.pages > 0 && io.out_of_time() {
            break;
        }
        let path = sec::advisories_poll_path(&start.since, after.as_deref());
        let answer = io.page(&path).await;
        report.pages += 1;
        match answer {
            Ok((code, body, link)) if (200..300).contains(&code) => {
                saved_cursor = false;
                let page = body.as_array().cloned().unwrap_or_default();
                report.read += page.len();
                let (matched, created) = io.apply(&page).await;
                report.matched += matched;
                report.created += created;
                let Some(next) = sec::dependabot_next_cursor(link.as_deref()) else {
                    report.caught_up = true;
                    break;
                };
                let (from, cursor) = sec::advisory_poll_checkpoint(&start.since, &page, &next);
                io.save_poll_fields(&[
                    ("advisory_poll_from", from),
                    ("advisory_poll_after", cursor.unwrap_or_default()),
                ])
                .await;
                after = Some(next);
            }
            // A kept cursor GitHub no longer takes: the same time without it.
            Ok((code, _, _)) if saved_cursor && matches!(code, 400 | 404 | 422) => {
                saved_cursor = false;
                after = None;
            }
            Ok((code, body, _)) => {
                let why = body["message"].as_str().unwrap_or("").trim();
                report.error = Some(if why.is_empty() {
                    format!("GitHub answered {code}")
                } else {
                    format!("GitHub answered {code}: {}", common::truncate_str(why, 200))
                });
                break;
            }
            Err(e) => {
                report.error = Some(format!(
                    "GitHub didn't answer: {}",
                    common::truncate_str(&e, 200)
                ));
                break;
            }
        }
    }
    let run_at = now.to_rfc3339();
    let mut fields: Vec<(&'static str, String)> = vec![
        ("advisory_poll_run_at", run_at.clone()),
        ("advisory_poll_caught_up", report.caught_up.to_string()),
        (
            "advisory_poll_error",
            report.error.clone().unwrap_or_default(),
        ),
    ];
    if report.caught_up {
        fields.extend([
            ("advisory_polled_at", run_at),
            ("advisory_poll_from", String::new()),
            ("advisory_poll_after", String::new()),
        ]);
    }
    io.save_poll_fields(&fields).await;
    report
}

/// The live [`AdvisoryPollIo`]: the team's GitHub token, its row, its
/// dependency lists (read once a poll) and its findings.
struct LiveAdvisoryPoll<'a, 'b> {
    cx: &'a Ctx<'b>,
    findings: &'a mut Vec<(Finding, FindingState)>,
    lists: Option<Vec<(String, Vec<Dep>)>>,
}

#[async_trait::async_trait]
impl AdvisoryPollIo for LiveAdvisoryPoll<'_, '_> {
    async fn poll_fields(&self) -> HashMap<String, String> {
        let row = get_team_state(self.cx.state, self.cx.team_id)
            .await
            .unwrap_or_default();
        POLL_FIELDS
            .iter()
            .map(|k| (k.to_string(), s(&row, k)))
            .collect()
    }

    async fn save_poll_fields(&self, fields: &[(&'static str, String)]) {
        let attrs: Vec<(&str, AttributeValue)> =
            fields.iter().map(|(k, v)| (*k, attr_s(v))).collect();
        set_team_state(self.cx.state, self.cx.team_id, &attrs).await;
    }

    async fn page(&self, path: &str) -> Result<(u16, Value, Option<String>), String> {
        self.cx
            .gh
            .get_status_link(path)
            .await
            .map_err(|e| e.to_string())
    }

    async fn apply(&mut self, advisories: &[Value]) -> (usize, usize) {
        if advisories.is_empty() {
            return (0, 0);
        }
        if self.lists.is_none() {
            let mut lists = vec![];
            for repo in team_repos(self.cx.state, self.cx.team_id, self.cx.cfg).await {
                if let Some((deps, _)) = load_deps(self.cx.state, self.cx.team_id, &repo).await {
                    lists.push((repo, deps));
                }
            }
            self.lists = Some(lists);
        }
        let lists = self.lists.as_deref().unwrap_or_default();
        let mut obs = vec![];
        for adv in advisories {
            for (repo, deps) in lists {
                obs.extend(sec::match_github_advisory(repo, adv, deps));
            }
            if adv["withdrawn_at"].is_string() {
                if let Some(ghsa) = adv["ghsa_id"].as_str() {
                    obs.extend(withdrawn_observations(self.findings, ghsa));
                }
            }
        }
        let matched = obs.len();
        if obs.is_empty() {
            return (0, 0);
        }
        let created = upsert(
            self.cx.state,
            self.cx.team_id,
            self.cx.cfg,
            self.findings,
            obs,
        )
        .await
        .created;
        (matched, created)
    }

    fn out_of_time(&self) -> bool {
        self.cx.out_of_time()
    }
}

/// The backstop for the `security_advisory` webhook (which a quiet day, or
/// an install the hosted relay doesn't forward global events to, never
/// sees): GitHub's advisories updated since the last poll, matched against
/// every repository's dependency list ([`run_advisory_poll`]).
async fn poll_advisories(cx: &Ctx<'_>, findings: &mut Vec<(Finding, FindingState)>) -> usize {
    let mut io = LiveAdvisoryPoll {
        cx,
        findings,
        lists: None,
    };
    let r = run_advisory_poll(&mut io, Utc::now()).await;
    let line = |r: &AdvisoryPollReport| {
        format!(
            "since={} pages={} read={} matched={} created={} caught_up={}",
            r.since, r.pages, r.read, r.matched, r.created, r.caught_up
        )
    };
    match &r.error {
        Some(e) => {
            warn!(team_id = cx.team_id, error = %e, poll = %line(&r), "security: advisory poll failed");
            record_problem(
                cx.state,
                cx.team_id,
                sec::STATE_SK,
                Check::AdvisoryPoll,
                Outcome::Fail(Kind::AdvisoryPollFailed, format!("The last read of GitHub's advisory database failed ({e}); it is tried again within the hour.")),
            )
            .await;
        }
        None => {
            info!(team_id = cx.team_id, poll = %line(&r), "security: advisory poll");
            record_problem(
                cx.state,
                cx.team_id,
                sec::STATE_SK,
                Check::AdvisoryPoll,
                Outcome::Pass,
            )
            .await;
        }
    }
    r.created
}

/// The scheduled sweep (on the review-reminder tick).
pub async fn sweep(state: &WorkerState) -> SweepReport {
    common::usage::with_feature(common::usage::feature::SECURITY, sweep_inner(state)).await
}

async fn sweep_inner(state: &WorkerState) -> SweepReport {
    let mut report = SweepReport::default();
    let teams = match common::settings_index::members(
        &state.dynamo,
        &state.config.settings_table_name,
        &sec::index_spec(),
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "security: the teams index could not be read");
            return report;
        }
    };
    let started = Instant::now();
    let mut kev_map: Option<BTreeMap<String, String>> = None;
    for team in teams {
        // The sources, triage and fix runs share the sweep's budget. The
        // release does not: a team reached after the budget is spent still
        // has its findings checked against GitHub, until the sweep's hard
        // stop.
        let spent = started.elapsed().as_secs();
        if spent >= TICK_BUDGET_SECS + RELEASE_BUDGET_SECS {
            break;
        }
        let heavy = spent < TICK_BUDGET_SECS;
        let Some(cfg) = load_config(state, &team).await else {
            continue;
        };
        // A team that turned security fixes off still sees its findings:
        // they are released once a day, and nothing else runs.
        if !cfg.enabled {
            let day = Utc::now().format("%Y%m%d").to_string();
            if !claim(state, &team, &format!("SECRELEASE#{day}"), 2 * 86_400).await {
                continue;
            }
        }
        let Some(iid) = super::review_reminder::team_installation_id(state, &team).await else {
            continue;
        };
        if heavy && cfg.enabled && kev_map.is_none() {
            kev_map = Some(kev(state).await);
        }
        let kev_ref = kev_map.as_ref().filter(|_| heavy && cfg.enabled);
        if cfg.enabled {
            report.teams += 1;
        }
        match team_tick(state, &team, iid, &cfg, kev_ref, started).await {
            Ok(r) => {
                report.released += r.released;
                report.repos_synced += r.repos_synced;
                report.new_findings += r.new_findings;
                report.triaged += r.triaged;
                report.runs += r.runs;
                report.cards += r.cards;
            }
            Err(e) => warn!(team_id = %team, error = %e, "security: team tick failed"),
        }
    }
    report
}

// ── Events ──────────────────────────────────────────────────────────────

/// A `security_event` from the gateway.
pub async fn on_event(state: &WorkerState, msg: &SecurityEventMessage) {
    common::usage::with_feature(common::usage::feature::SECURITY, on_event_inner(state, msg)).await
}

async fn on_event_inner(state: &WorkerState, msg: &SecurityEventMessage) {
    let release = matches!(msg.kind.as_str(), "pr" | "run" | "retry");
    let teams: Vec<String> = if msg.team_id.is_empty() {
        common::settings_index::members(
            &state.dynamo,
            &state.config.settings_table_name,
            &sec::index_spec(),
        )
        .await
        .unwrap_or_default()
    } else {
        vec![msg.team_id.clone()]
    };
    // A release reads GitHub and the run records only; the rest tends the
    // findings afterwards and needs the CISA list.
    let kev_map = if matches!(msg.kind.as_str(), "pr" | "run") {
        BTreeMap::new()
    } else {
        kev(state).await
    };
    for team in teams {
        // A release is the truth about a finding, not an action: it runs
        // for a team that turned security fixes off too.
        let Some(cfg) = load_config(state, &team)
            .await
            .filter(|c| c.enabled || release)
        else {
            continue;
        };
        let iid = if msg.installation_id > 0 && !msg.team_id.is_empty() {
            Some(msg.installation_id)
        } else {
            super::review_reminder::team_installation_id(state, &team).await
        };
        let Some(iid) = iid else { continue };
        if let Err(e) = event_for_team(state, &team, iid, &cfg, &kev_map, msg).await {
            warn!(team_id = %team, kind = %msg.kind, error = %e, "security: event failed");
        }
    }
}

async fn event_for_team(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    cfg: &SecurityConfig,
    kev_map: &BTreeMap<String, String>,
    msg: &SecurityEventMessage,
) -> Result<(), Error> {
    let cx = Ctx {
        state,
        team_id,
        cfg,
        gh: github(state, installation_id)?,
        started: Instant::now(),
        osv: Default::default(),
    };
    let mut findings = load_findings(state, team_id).await;
    let mut report = TeamReport::default();
    match msg.kind.as_str() {
        "dependabot_alert" => {
            if !sec::valid_repo(&msg.repo) || !cfg.repos.includes(&msg.repo) {
                return Ok(());
            }
            let alert = cx
                .gh
                .get_api(&format!(
                    "/repos/{}/dependabot/alerts/{}",
                    msg.repo, msg.alert_number
                ))
                .await?;
            let mut obs =
                sec::observation_from_alert(&msg.repo, &alert).ok_or("unreadable alert")?;
            if msg.action == "withdrawn" {
                obs.advisory.withdrawn = true;
            }
            let deps = load_deps(state, team_id, &msg.repo)
                .await
                .map(|(d, _)| d)
                .unwrap_or_default();
            fill_version(&mut obs, &deps);
            upsert(state, team_id, cfg, &mut findings, vec![obs]).await;
        }
        "security_advisory" => {
            if msg.ghsa_id.is_empty() {
                return Ok(());
            }
            let adv = cx
                .gh
                .get_api(&format!(
                    "/advisories/{}",
                    urlencoding::encode(&msg.ghsa_id)
                ))
                .await?;
            let mut obs = vec![];
            for repo in team_repos(state, team_id, cfg).await {
                if let Some((deps, _)) = load_deps(state, team_id, &repo).await {
                    obs.extend(sec::match_github_advisory(&repo, &adv, &deps));
                }
            }
            if msg.action == "withdrawn" || adv["withdrawn_at"].is_string() {
                obs.extend(withdrawn_observations(&findings, &msg.ghsa_id));
            }
            let applied = upsert(state, team_id, cfg, &mut findings, obs).await;
            set_team_state(
                state,
                team_id,
                &[("advisory_event_at", attr_s(&Utc::now().to_rfc3339()))],
            )
            .await;
            report.new_findings += applied.created;
        }
        "push" => {
            if sec::valid_repo(&msg.repo) && cfg.repos.includes(&msg.repo) {
                sync_repo(&cx, &msg.repo, &mut findings, false).await;
            }
        }
        "scan" => {
            let repos = if msg.repo.is_empty() {
                team_repos(state, team_id, cfg).await
            } else {
                vec![msg.repo.clone()]
            };
            let day = Utc::now().format("%Y%m%d").to_string();
            for repo in repos {
                if cx.out_of_time() {
                    break;
                }
                report.new_findings += sync_repo(&cx, &repo, &mut findings, true).await;
                // Read now: the day's sweep needn't read it again.
                claim(
                    state,
                    team_id,
                    &format!("SECSYNC#{}#{day}", repo.to_ascii_lowercase()),
                    2 * 86_400,
                )
                .await;
            }
        }
        // A security pull request closed or reopened (the gateway's
        // webhook), or a person cancelled or closed a fix run: the findings
        // follow, now.
        "pr" | "run" => {
            let scope = if msg.kind == "pr" {
                release::Scope::Pr {
                    repo: msg.repo.clone(),
                    number: msg.pr_number,
                }
            } else {
                release::Scope::Ticket(msg.ticket.clone())
            };
            let r = release::run(&cx, &mut findings, &scope, release::TEAM_BUDGET_SECS).await;
            info!(team_id, kind = %msg.kind, repo = %msg.repo, pr = msg.pr_number, ticket = %msg.ticket, changed = r.changed(), "security: release event handled");
            return Ok(());
        }
        // A person pressed Retry on a fix run whose pull request is gone:
        // its findings, as they stand now, are asked for again, and the
        // request starts below like any other a person made.
        "retry" => {
            retry_asked(&cx, &mut findings, &msg.ticket, &msg.requested_by).await;
        }
        "fix" => {
            report.runs += start_asked(
                &cx,
                &mut findings,
                &msg.finding_ids,
                &msg.requested_by,
                msg.combine,
            )
            .await;
        }
        other => {
            info!(kind = other, "security: unknown event kind");
            return Ok(());
        }
    }
    tend(&cx, &mut findings, kev_map, false, &mut report).await;
    if cfg.acts() {
        close_withdrawn(&cx, &mut findings).await;
    }
    info!(team_id, kind = %msg.kind, new = report.new_findings, triaged = report.triaged, runs = report.runs, cards = report.cards, "security: event handled");
    Ok(())
}

// ── The run's pull request ──────────────────────────────────────────────

/// A security run's plan, when the ticket is one.
async fn load_unit(state: &WorkerState, team_id: &str, ticket: &str) -> Option<FixUnit> {
    if !sec::is_ticket(ticket) {
        return None;
    }
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sec::ticket_sk(ticket)))
        .send()
        .await
        .ok()?;
    serde_json::from_str(&s(out.item()?, "unit")).ok()
}

async fn unit_findings(
    state: &WorkerState,
    team_id: &str,
    unit: &FixUnit,
) -> Vec<(Finding, FindingState)> {
    let mut out = vec![];
    for id in &unit.finding_ids {
        let Ok(o) = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(team_id))
            .key("sk", attr_s(&sec::finding_sk(id)))
            .send()
            .await
        else {
            continue;
        };
        if let Some(i) = o.item() {
            if let Ok(f) = serde_json::from_str::<Finding>(&s(i, "finding")) {
                let st = serde_json::from_str(&s(i, "state")).unwrap_or_default();
                out.push((f, st));
            }
        }
    }
    out
}

/// The description of a security run's pull request, the run's own words
/// under Change. `None` for any other run.
pub async fn pr_body(state: &WorkerState, msg: &TicketMessage, change: &str) -> Option<String> {
    let unit = load_unit(state, &msg.team_id, &msg.ticket_id).await?;
    if unit.ecosystem == common::infra_vuln::records::UNIT_ECOSYSTEM {
        return super::infra_findings::pr_body(state, &msg.team_id, &unit, change).await;
    }
    // Kept, so the description can be written again when fixes join.
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&sec::ticket_sk(&msg.ticket_id)))
        .update_expression("SET #c = :c")
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_names("#c", "change")
        .expression_attribute_values(":c", attr_s(common::truncate_str(change, 20_000)))
        .send()
        .await;
    let members = unit_findings(state, &msg.team_id, &unit).await;
    let pairs: Vec<(&Finding, &FindingState)> = members.iter().map(|(f, s)| (f, s)).collect();
    let item = ticket_item(state, &msg.team_id, &msg.ticket_id).await;
    let notes = item.as_ref().map(|i| s(i, "notes")).unwrap_or_default();
    let gh = github(state, msg.installation_id).ok();
    let mut check = match &gh {
        Some(gh) => what_to_check(gh, &unit, &notes, None).await,
        None => common::upgrade_notes::WhatToCheck::from_notes(&notes),
    };
    check.dep_report = item
        .as_ref()
        .map(|i| s(i, "dep_report"))
        .unwrap_or_default();
    check.upgrade_facts = item
        .as_ref()
        .and_then(|i| common::upgrade_quality::UpgradeFacts::from_json(&s(i, "dep_facts")));
    let mut links = body_links(state, item.as_ref());
    // A bundle too long to describe in full keeps its full detail on its
    // branch, beside the plan CoderHelm commits there, when the team lets
    // CoderHelm commit those files.
    if sec::uses_short_body(&unit, &pairs) {
        if let Some(gh) = &gh {
            if let Some(url) = keep_detail(state, gh, msg, &unit, &pairs, change, &check).await {
                links.detail = url;
            }
        }
    }
    // The table and the headline say what the branch's lockfile resolves,
    // never what the run planned.
    let head = match &gh {
        Some(gh) => head_facts(gh, &unit, &msg.base_branch, &unit.branch).await,
        None => common::dep_head::unknown(&unit, "GitHub could not be reached to read the branch"),
    };
    Some(sec::pr_body_at_head(
        &unit, &pairs, change, &check, &links, &head,
    ))
}

/// The lockfiles whose resolved copies CoderHelm reads.
fn is_read_lockfile(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    !path.contains("node_modules/")
        && (common::reachability::is_lockfile(path)
            || matches!(
                name,
                "Cargo.lock" | "poetry.lock" | "uv.lock" | "Gemfile.lock"
            ))
}

/// What the head of a unit's branch resolves for each of its bumps: the
/// lockfiles its bumps go through, read on the base branch and on the head
/// and compared (`common::dep_head`). Whatever can't be read is said as not
/// known, with why.
pub(crate) async fn head_facts(
    gh: &GitHubClient,
    unit: &FixUnit,
    base: &str,
    head: &str,
) -> common::dep_head::HeadFacts {
    use common::dep_head::{self, LockAt};
    let Some((o, n)) = unit.repo.split_once('/') else {
        return dep_head::unknown(unit, "the repository is not known");
    };
    if head.is_empty() {
        return dep_head::unknown(unit, "the pull request's branch is not known");
    }
    let tree = match gh.get_tree(o, n, head).await {
        Ok(t) => t,
        Err(e) => {
            warn!(repo = %unit.repo, error = %e, "security: could not list the head's files");
            return dep_head::unknown(unit, "the files on the head could not be listed");
        }
    };
    let lockfiles: Vec<String> = tree
        .into_iter()
        .map(|t| t.path)
        .filter(|p| is_read_lockfile(p))
        .collect();
    let mut wanted: Vec<String> = vec![];
    for b in &unit.bumps {
        let lock =
            common::dep_fix::lockfile_for(&b.manifest, &lockfiles).or_else(|| {
                match lockfiles.as_slice() {
                    [one] => Some(one.clone()),
                    _ => None,
                }
            });
        if let Some(l) = lock {
            if !wanted.contains(&l) {
                wanted.push(l);
            }
        }
    }
    let mut locks = vec![];
    for path in wanted {
        let mut at = LockAt {
            path: path.clone(),
            ..Default::default()
        };
        for (side, git_ref) in [("the base branch", base), ("the head", head)] {
            let copies = match gh.read_file_if_exists(o, n, &path, git_ref).await {
                // Not there: nothing is installed through it on that side.
                Ok(None) => Some(vec![]),
                Ok(Some(text)) => {
                    let c = common::reachability::lock_copies(&path, &text);
                    if c.is_none() {
                        at.why = format!("`{path}` on {side} could not be read as a lockfile");
                    }
                    c
                }
                Err(e) => {
                    at.why = format!("`{path}` could not be read on {side} ({e})");
                    None
                }
            };
            if side == "the head" {
                at.head = copies;
            } else {
                at.base = copies;
            }
        }
        locks.push(at);
    }
    let sha = gh.get_ref(o, n, head).await.unwrap_or_default();
    dep_head::resolve(unit, &sha, &locks)
}

/// Where a security description points for what it leaves out: each
/// finding's page in CoderHelm, and the full detail kept on the branch (its
/// link recorded on the ticket when it was written).
fn body_links(state: &WorkerState, item: Option<&Item>) -> sec::BodyLinks {
    sec::BodyLinks {
        finding: format!(
            "{}/security/detail?id=",
            super::skills::app_url(&state.config.stage)
        ),
        detail: item.map(|i| s(i, "detail_url")).unwrap_or_default(),
    }
}

/// The link to a file on a branch on GitHub.
pub fn blob_url(repo: &str, branch: &str, path: &str) -> String {
    format!("https://github.com/{repo}/blob/{branch}/{path}")
}

/// Writes the full detail of a bundle (every package's notes, every
/// advisory) to `openspec/specs/<ticket>/security-detail.md` on its branch,
/// where CoderHelm keeps the run's plan, and records its link on the
/// ticket. Only when the team lets CoderHelm commit those files
/// (`commit_openspec`); a file already as written is left alone.
async fn keep_detail(
    state: &WorkerState,
    gh: &GitHubClient,
    msg: &TicketMessage,
    unit: &FixUnit,
    pairs: &[(&Finding, &FindingState)],
    change: &str,
    check: &common::upgrade_notes::WhatToCheck,
) -> Option<String> {
    if unit.branch.is_empty()
        || !super::load_workflow_setting(state, &msg.team_id, "commit_openspec").await
    {
        return None;
    }
    let (o, n) = unit.repo.split_once('/')?;
    let path = sec::detail_path(&msg.ticket_id);
    let doc = sec::detail_doc(unit, pairs, change, check);
    let same = gh
        .read_file(o, n, &path, &unit.branch)
        .await
        .is_ok_and(|now| now == doc);
    if !same {
        let conventions = super::load_git_conventions(state, &msg.team_id, &msg.ticket_id).await;
        let message = conventions.commit_message(
            &super::ticket_tokens(msg),
            &format!("Add the full detail of {}", msg.ticket_id),
        );
        if let Err(e) = gh
            .batch_write(
                o,
                n,
                &unit.branch,
                &message,
                &[crate::clients::github::FileOp::Write {
                    path: path.clone(),
                    content: doc,
                }],
            )
            .await
        {
            warn!(repo = %unit.repo, error = %e, "security: could not keep the full detail on the branch");
            return None;
        }
    }
    let url = blob_url(&unit.repo, &unit.branch, &path);
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&sec::ticket_sk(&msg.ticket_id)))
        .update_expression("SET detail_url = :u")
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_values(":u", attr_s(&url))
        .send()
        .await;
    Some(url)
}

/// Keeps a ticket's upgrade notes.
async fn save_notes(
    state: &WorkerState,
    team_id: &str,
    ticket: &str,
    notes: &[common::upgrade_notes::PackageNotes],
) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sec::ticket_sk(ticket)))
        .update_expression("SET notes = :n")
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_values(
            ":n",
            attr_s(&serde_json::to_string(notes).unwrap_or_default()),
        )
        .send()
        .await;
}

/// The pull request's CI for its "What to check": which checks ran and
/// which fail (CoderHelm's own checks left out).
async fn ci_of(gh: &GitHubClient, repo: &str, number: u64) -> common::upgrade_notes::CiState {
    let mut out = common::upgrade_notes::CiState::default();
    let Some((o, n)) = repo.split_once('/') else {
        return out;
    };
    let Ok(pr) = gh.get_pull_request(o, n, number).await else {
        return out;
    };
    let sha = pr["head"]["sha"].as_str().unwrap_or("");
    let Ok(v) = gh.list_ci_for_ref(o, n, sha).await else {
        return out;
    };
    let runs: Vec<Value> = v["check_runs"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|r| {
            let name = r["name"].as_str().unwrap_or("");
            name != common::review_check::CHECK_NAME
                && !super::is_own_login(r["app"]["slug"].as_str().unwrap_or(""))
        })
        .collect();
    if runs.is_empty() {
        return out;
    }
    for r in &runs {
        let name = r["name"].as_str().unwrap_or("").to_string();
        if !out.checks.contains(&name) {
            out.checks.push(name.clone());
        }
        if matches!(
            r["conclusion"].as_str(),
            Some("failure" | "timed_out" | "cancelled" | "action_required")
        ) && !out.failing.contains(&name)
        {
            out.failing.push(name);
        }
    }
    out.state = if !out.failing.is_empty() {
        "failing"
    } else if runs
        .iter()
        .all(|r| r["status"].as_str() == Some("completed"))
    {
        "passing"
    } else {
        "pending"
    }
    .into();
    out
}

/// What a security pull request's "What to check" is written from: the
/// ticket's upgrade notes, the files the branch changes, and the pull
/// request's CI when it has one.
async fn what_to_check(
    gh: &GitHubClient,
    unit: &FixUnit,
    notes: &str,
    pr: Option<u64>,
) -> common::upgrade_notes::WhatToCheck {
    let mut w = common::upgrade_notes::WhatToCheck::from_notes(notes);
    if let Some((o, n)) = unit.repo.split_once('/') {
        if let Ok(base) = gh.get_default_branch(o, n).await {
            if let Ok(files) = gh.compare_changed_files(o, n, &base, &unit.branch).await {
                w.files = files;
            }
        }
    }
    if let Some(number) = pr {
        w.ci = ci_of(gh, &unit.repo, number).await;
    }
    w
}

/// A security pull request whose CI just ended (passed or failed): its
/// description is rebuilt from its head.
async fn refresh_ci_sections(cx: &Ctx<'_>, repo: &str, number: u64, ticket: &str) {
    let Some((o, n)) = repo.split_once('/') else {
        return;
    };
    crate::passes::description::rebuild(cx.state, &cx.gh, cx.team_id, o, n, number, ticket).await;
}

/// A security pull request's description with the sections CoderHelm
/// computes written again from the head `pr` is on: the first sentences
/// and the table (what the lockfile on the head resolves, compared with
/// the base branch), "What to check" and Proof (where CI stands on this
/// head). The rest (the draft status at its top, the run's words) stays.
/// `None` for a pull request that is not a dependency fix's. Saved by
/// `description::reconcile`, the only caller.
pub(crate) async fn at_head(
    state: &WorkerState,
    gh: &GitHubClient,
    team_id: &str,
    repo: &str,
    number: u64,
    ticket: &str,
    pr: &Value,
) -> Option<String> {
    let unit = load_unit(state, team_id, ticket).await?;
    if unit.ecosystem == common::infra_vuln::records::UNIT_ECOSYSTEM {
        return None;
    }
    let body = pr["body"].as_str().unwrap_or("");
    if !body.contains(sec::MARKER) {
        return None;
    }
    let item = ticket_item(state, team_id, ticket).await;
    let change = item.as_ref().map(|i| s(i, "change")).unwrap_or_default();
    let notes = item.as_ref().map(|i| s(i, "notes")).unwrap_or_default();
    let members = unit_findings(state, team_id, &unit).await;
    let pairs: Vec<(&Finding, &FindingState)> = members.iter().map(|(f, st)| (f, st)).collect();
    let mut check = what_to_check(gh, &unit, &notes, Some(number)).await;
    check.dep_report = item
        .as_ref()
        .map(|i| s(i, "dep_report"))
        .unwrap_or_default();
    check.upgrade_facts = item
        .as_ref()
        .and_then(|i| common::upgrade_quality::UpgradeFacts::from_json(&s(i, "dep_facts")));
    // The same links as when it opened: the rewritten sections are the
    // short ones a bundle's description carries.
    let links = body_links(state, item.as_ref());
    let head = head_facts(
        gh,
        &unit,
        pr["base"]["ref"].as_str().unwrap_or(""),
        pr["head"]["sha"]
            .as_str()
            .filter(|s| !s.is_empty())
            .unwrap_or(&unit.branch),
    )
    .await;
    let fresh = sec::pr_body_at_head(&unit, &pairs, &change, &check, &links, &head);
    let _ = repo;
    Some(rebuilt_body(body, &fresh))
}

/// `body` with the sections computed from the head taken from `fresh`.
/// Every section CoderHelm owns elsewhere (the draft status, review fixes,
/// the settled items, CI notes, the reviewer guide, open items and the
/// rest of `completeness::Section`) is lifted out first and put back as it
/// was, so a rebuild can never overwrite one that sits under a rebuilt
/// heading. A body the rebuild does not change is returned as it is.
pub(crate) fn rebuilt_body(body: &str, fresh: &str) -> String {
    let (owned, rest) = super::completeness::take_owned(body);
    let next = common::upgrade_notes::replace_sections(
        &rest,
        fresh,
        &["## Summary", "## What to check", "## Proof"],
    );
    if next == rest {
        return body.to_string();
    }
    super::completeness::assemble(&next, &owned)
}

/// A security run opened its pull request: labels, the findings, the card.
pub async fn on_pr_opened(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &TicketMessage,
    number: u64,
    url: &str,
) {
    let Some(unit) = load_unit(state, &msg.team_id, &msg.ticket_id).await else {
        return;
    };
    if let Err(e) = github
        .add_labels(&msg.repo_owner, &msg.repo_name, number, &unit.labels)
        .await
    {
        warn!(number, error = %e, "security: could not label the pull request");
    }
    if unit.ecosystem == common::infra_vuln::records::UNIT_ECOSYSTEM {
        super::infra_findings::on_pr_opened(state, &msg.team_id, &unit, number, url).await;
        return;
    }
    let mut members = unit_findings(state, &msg.team_id, &unit).await;
    let cfg = load_config(state, &msg.team_id).await.unwrap_or_default();
    let channels = if cfg.acts() {
        team_channels(state, &msg.team_id).await
    } else {
        Default::default()
    };
    let mut carded = false;
    for (f, st) in members.iter_mut() {
        // Told already (the pull request was there before this run).
        if f.pr
            .as_ref()
            .is_some_and(|p| p.number == number && p.state == "open")
        {
            continue;
        }
        // Fixed, dismissed or withdrawn while the run worked: it stays so.
        if f.status.is_handled() {
            continue;
        }
        f.status = Status::PrOpen;
        f.waiting = None;
        st.hold_until = None;
        f.pr = Some(sec::PrRef {
            number,
            url: url.to_string(),
            ci: Some("pending".into()),
            state: "open".into(),
            adopted: false,
            ..Default::default()
        });
        st.pr_opened_at
            .get_or_insert_with(|| Utc::now().to_rfc3339());
        f.updated_at = Utc::now().to_rfc3339();
        save_finding(state, &msg.team_id, f, st).await;
        add_event(
            state,
            &msg.team_id,
            &f.id,
            &format!("PR #{number} opened"),
            Some(url),
        )
        .await;
        if !carded && st.cards.iter().any(|c| c == "new") {
            let thing = format!("PR#{}#{number}", unit.repo.to_ascii_lowercase());
            carded = post_card(state, &msg.team_id, &channels, &thing, Stage::PrOpen, f).await;
        }
    }
}

// ── Dependency fix runs (`passes::dep_fix`) ─────────────────────────────

/// What a dependency fix run plans from: the ticket's unit, its findings
/// and the team's policy.
pub(crate) struct DepInputs {
    pub findings: Vec<(Finding, FindingState)>,
    pub cfg: SecurityConfig,
}

pub(crate) async fn dep_inputs(
    state: &WorkerState,
    team_id: &str,
    ticket: &str,
) -> Option<DepInputs> {
    let unit = load_unit(state, team_id, ticket).await?;
    if unit.ecosystem == common::infra_vuln::records::UNIT_ECOSYSTEM
        || unit.ecosystem == common::base_images::ECOSYSTEM
    {
        return None;
    }
    let cfg = load_config(state, team_id).await.unwrap_or_default();
    let findings = unit_findings(state, team_id, &unit).await;
    (!findings.is_empty()).then_some(DepInputs { findings, cfg })
}

/// The upgrade evidence a dependency run recorded for its head commit
/// (`passes::upgrade_quality::record`), when it recorded one.
pub(crate) async fn dep_facts(
    state: &WorkerState,
    team_id: &str,
    ticket: &str,
) -> Option<common::upgrade_quality::UpgradeFacts> {
    let item = ticket_item(state, team_id, ticket).await?;
    common::upgrade_quality::UpgradeFacts::from_json(&s(&item, "dep_facts"))
}

/// The upgrade notes read for the ticket before its run.
pub(crate) async fn dep_notes(
    state: &WorkerState,
    team_id: &str,
    ticket: &str,
) -> Vec<common::upgrade_notes::PackageNotes> {
    let notes = ticket_item(state, team_id, ticket)
        .await
        .map(|i| s(&i, "notes"))
        .unwrap_or_default();
    serde_json::from_str(&notes).unwrap_or_default()
}

/// A dependency run's end: its deferred findings go back to open with the
/// reason as their wait and leave the unit (the ones the run itself found
/// breaking, `held`, stay out of later bundles while the deferral holds);
/// the unit keeps what the pull request fixes, and the run's report is kept
/// for the description.
pub(crate) async fn settle_dep_run(
    state: &WorkerState,
    team_id: &str,
    ticket: &str,
    fixed: &[String],
    deferred: &[(String, String, bool)],
    held: &[String],
    report: &str,
) {
    let Some(unit) = load_unit(state, team_id, ticket).await else {
        return;
    };
    let mut findings = unit_findings(state, team_id, &unit).await;
    let now = Utc::now();
    let gone: Vec<String> = deferred
        .iter()
        .map(|(id, _, _)| id.clone())
        .filter(|id| !fixed.contains(id))
        .collect();
    for (f, st) in findings.iter_mut() {
        let Some((_, why, _)) = deferred.iter().find(|(id, _, _)| id == &f.id) else {
            continue;
        };
        if fixed.contains(&f.id) {
            continue;
        }
        f.status = Status::Open;
        f.waiting = Some(why.clone());
        f.updated_at = now.to_rfc3339();
        st.ticket = None;
        st.bundled = false;
        if held.contains(&f.id) {
            st.deferred = Some(common::dep_fix::RunDeferral::new(f, why, now));
        }
        save_finding(state, team_id, f, st).await;
    }
    if !gone.is_empty() {
        let cfg = load_config(state, team_id).await.unwrap_or_default();
        let all: Vec<(Finding, FindingState)> = findings.clone();
        let kept = sec::without(&unit, &gone, &all, &cfg);
        if let Err(e) = save_unit(state, team_id, ticket, &kept).await {
            warn!(team_id, ticket, error = %e, "security: the unit without its deferred bumps could not be saved");
        }
    }
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sec::ticket_sk(ticket)))
        .update_expression("SET dep_report = :r")
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_values(":r", attr_s(common::truncate_str(report, 20_000)))
        .send()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_open_bot_pr_for_the_package_is_adopted() {
        let unit = FixUnit {
            bumps: vec![sec::Bump {
                package: "undici".into(),
                from: "6.19.2".into(),
                to: "6.21.1".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let pulls = vec![
            json!({"number": 1, "head": {"ref": "feature/x"}, "title": "Bump undici from 6.19.2 to 6.21.1"}),
            json!({"number": 2, "head": {"ref": "dependabot/npm_and_yarn/undici-6.21.1"}, "title": "Bump undici from 6.19.2 to 6.21.1"}),
        ];
        assert_eq!(bot_pr_for(&pulls, &unit).unwrap()["number"], 2);
        let other = vec![
            json!({"number": 3, "head": {"ref": "renovate/tar-7.x"}, "title": "Update dependency tar to v7"}),
        ];
        assert!(bot_pr_for(&other, &unit).is_none());
    }

    #[test]
    fn scopes_from_a_lockfile_set_the_manifest() {
        let mut deps = vec![
            Dep {
                ecosystem: "npm".into(),
                name: "jest".into(),
                version: "29.0.0".into(),
                manifest: sec::SBOM_MANIFEST.into(),
                scope: Scope::Unknown,
                path: vec![],
            },
            Dep {
                ecosystem: "PyPI".into(),
                name: "Django".into(),
                version: "4.2".into(),
                manifest: sec::SBOM_MANIFEST.into(),
                scope: Scope::Unknown,
                path: vec![],
            },
        ];
        let scopes = HashMap::from([("jest".to_string(), Scope::Dev)]);
        apply_scopes(&mut deps, "npm", "web/package-lock.json", &scopes);
        assert_eq!(deps[0].scope, Scope::Dev);
        assert_eq!(deps[0].manifest, "web/package-lock.json");
        apply_scopes(
            &mut deps,
            "PyPI",
            "poetry.lock",
            &HashMap::from([("django".to_string(), Scope::Runtime)]),
        );
        assert_eq!(deps[1].scope, Scope::Runtime);
        assert!(is_lockfile_or_manifest("go.sum") && !is_lockfile_or_manifest("src/a.ts"));
    }

    fn open_finding(id: &str, tier: sec::Tier, cvss: f64, scope: Scope) -> (Finding, FindingState) {
        (
            Finding {
                id: id.into(),
                repo: "acme/web".into(),
                ecosystem: "npm".into(),
                package: id.into(),
                version: "1.0.0".into(),
                advisory: sec::Advisory {
                    ids: vec![format!("GHSA-{id}")],
                    cvss: Some(cvss),
                    ..Default::default()
                },
                scope,
                tier: Some(tier),
                verdict: Verdict::Unknown,
                ..Default::default()
            },
            FindingState::default(),
        )
    }

    /// Verdicts settled by the old way of reading (every "unknown" from the
    /// quoted search) are read again once, and every verdict weekly.
    #[test]
    fn old_and_week_old_verdicts_are_read_again() {
        let now = Utc::now();
        let (f, mut st) = open_finding("a", sec::Tier::P1, 9.8, Scope::Runtime);
        // What the old code stored: a key without the version, no time.
        st.triaged = Some(common::content_hash(&format!(
            "{}|{}|{:?}",
            f.advisory.ids.join(","),
            f.version,
            f.scope
        )));
        assert!(triage_due(&f, &st, now), "read by the old way: read again");
        st.triaged = Some(triage_key(&f));
        st.triaged_at = Some((now - Duration::days(1)).to_rfc3339());
        assert!(!triage_due(&f, &st, now));
        st.triaged_at = Some((now - Duration::days(8)).to_rfc3339());
        assert!(triage_due(&f, &st, now), "a week old: read again");
    }

    /// Criticals before highs, runtime before dev; and one tick reads every
    /// group an install has (140 there), with the model calls the day's cap
    /// allows: 50 model reads finish in five ticks, under 400k tokens.
    #[test]
    fn a_tick_reads_every_group_most_urgent_first() {
        let findings = vec![
            open_finding("high", sec::Tier::P1, 7.5, Scope::Runtime),
            open_finding("dev", sec::Tier::P1, 9.8, Scope::Dev),
            open_finding("crit", sec::Tier::P1, 9.8, Scope::Runtime),
            open_finding("low", sec::Tier::P3, 3.0, Scope::Runtime),
        ];
        let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (i, (f, _)) in findings.iter().enumerate() {
            groups.entry(sec::group_key(f)).or_default().push(i);
        }
        let order: Vec<String> = triage_order(&findings, &groups)
            .iter()
            .map(|k| k.split('|').nth(2).unwrap_or("").to_string())
            .collect();
        assert_eq!(order, ["crit", "dev", "high", "low"]);
        let per_tick = std::hint::black_box(TRIAGE_SEARCH_PER_TICK);
        assert!(per_tick >= 140, "every group in one tick");
        let estimate = common::reachability::estimate_call_tokens(TRIAGE_MAX_CHARS);
        let (mut left, mut spent, mut ticks) = (50usize, 0u64, 0);
        while left > 0 {
            ticks += 1;
            let n = common::reachability::model_calls_allowed(
                spent,
                sec::TRIAGE_TOKENS_PER_DAY,
                estimate,
                TRIAGE_PER_TICK,
            )
            .min(left);
            assert!(n > 0, "stalled under the cap after {spent} tokens");
            left -= n;
            spent += n as u64 * estimate;
        }
        assert!(spent <= sec::TRIAGE_TOKENS_PER_DAY);
        assert!(ticks <= 5, "{ticks} ticks");
    }

    #[test]
    fn the_lockfile_for_a_manifest_is_found() {
        let tree: Vec<String> = [
            "pnpm-lock.yaml",
            "lambdas/resizer/package-lock.json",
            "lambdas/resizer/package.json",
            "packages/ui/package.json",
            "node_modules/x/yarn.lock",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(lockfiles_for("pnpm-lock.yaml", &tree), ["pnpm-lock.yaml"]);
        assert_eq!(
            lockfiles_for("lambdas/resizer/package.json", &tree),
            ["lambdas/resizer/package-lock.json"]
        );
        assert_eq!(
            lockfiles_for("packages/ui/package.json", &tree),
            ["pnpm-lock.yaml"]
        );
        assert_eq!(
            lockfiles_for(sec::SBOM_MANIFEST, &tree),
            ["pnpm-lock.yaml", "lambdas/resizer/package-lock.json"]
        );
    }
}

#[cfg(test)]
mod advisory_poll_tests {
    use super::*;
    use std::sync::Mutex;

    const PER_PAGE: usize = 100;

    fn at(t: &str) -> chrono::DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339(t)
            .unwrap()
            .with_timezone(&Utc)
    }

    /// GitHub's `/advisories?updated=>=X&sort=updated&direction=asc`: each
    /// cursor is bound to the query's time (another time answers 422), and
    /// requests can be made to fail.
    struct FakeGitHub {
        /// (index, updated_at), ascending.
        advisories: Vec<(usize, chrono::DateTime<Utc>)>,
        row: Mutex<HashMap<String, String>>,
        /// Every path asked for, and the index of each advisory read.
        paths: Mutex<Vec<String>>,
        read: Mutex<Vec<usize>>,
        /// This run's requests, and the one that fails (1-based).
        requests: Mutex<usize>,
        fail_at: Option<usize>,
        /// Requests after which the sweep's time is up.
        time_for: Option<usize>,
    }

    impl FakeGitHub {
        fn new(advisories: Vec<(usize, chrono::DateTime<Utc>)>) -> Self {
            Self {
                advisories,
                row: Mutex::new(HashMap::new()),
                paths: Mutex::new(vec![]),
                read: Mutex::new(vec![]),
                requests: Mutex::new(0),
                fail_at: None,
                time_for: None,
            }
        }

        /// A new run: the row is kept, the run's counters reset.
        fn next_run(&mut self, time_for: Option<usize>, fail_at: Option<usize>) {
            *self.requests.lock().unwrap() = 0;
            self.paths.lock().unwrap().clear();
            self.read.lock().unwrap().clear();
            self.time_for = time_for;
            self.fail_at = fail_at;
        }

        fn field(&self, k: &str) -> String {
            self.row.lock().unwrap().get(k).cloned().unwrap_or_default()
        }

        fn read(&self) -> Vec<usize> {
            self.read.lock().unwrap().clone()
        }
    }

    fn query(path: &str) -> (String, Option<String>) {
        let q = path.split_once('?').unwrap().1;
        let mut since = String::new();
        let mut after = None;
        for kv in q.split('&') {
            let (k, v) = kv.split_once('=').unwrap();
            let v = urlencoding::decode(v).unwrap().into_owned();
            match k {
                "updated" => since = v.trim_start_matches(">=").to_string(),
                "after" => after = Some(v),
                _ => {}
            }
        }
        (since, after)
    }

    #[async_trait::async_trait]
    impl AdvisoryPollIo for FakeGitHub {
        async fn poll_fields(&self) -> HashMap<String, String> {
            self.row.lock().unwrap().clone()
        }

        async fn save_poll_fields(&self, fields: &[(&'static str, String)]) {
            let mut row = self.row.lock().unwrap();
            for (k, v) in fields {
                row.insert(k.to_string(), v.clone());
            }
        }

        async fn page(&self, path: &str) -> Result<(u16, Value, Option<String>), String> {
            self.paths.lock().unwrap().push(path.to_string());
            let n = {
                let mut r = self.requests.lock().unwrap();
                *r += 1;
                *r
            };
            if self.fail_at == Some(n) {
                return Ok((502, json!({ "message": "Server Error" }), None));
            }
            let (since, after) = query(path);
            let from = at(&since);
            let matching: Vec<&(usize, chrono::DateTime<Utc>)> =
                self.advisories.iter().filter(|(_, t)| *t >= from).collect();
            let offset = match after {
                None => 0,
                Some(c) => {
                    let (o, bound) = c.split_once('@').unwrap();
                    if bound != since {
                        return Ok((422, json!({ "message": "Invalid cursor" }), None));
                    }
                    o.parse::<usize>().unwrap()
                }
            };
            let page: Vec<&&(usize, chrono::DateTime<Utc>)> =
                matching.iter().skip(offset).take(PER_PAGE).collect();
            self.read
                .lock()
                .unwrap()
                .extend(page.iter().map(|(i, _)| *i));
            let body: Vec<Value> = page
                .iter()
                .map(|(i, t)| json!({ "ghsa_id": format!("GHSA-{i}"), "updated_at": poll_t(t) }))
                .collect();
            let next = offset + PER_PAGE;
            let link = (next < matching.len()).then(|| {
                format!(
                    "<https://api.github.com/advisories?per_page=100&after={}>; rel=\"next\"",
                    urlencoding::encode(&format!("{next}@{since}"))
                )
            });
            Ok((200, Value::Array(body), link))
        }

        async fn apply(&mut self, advisories: &[Value]) -> (usize, usize) {
            (advisories.len(), 0)
        }

        fn out_of_time(&self) -> bool {
            self.time_for
                .is_some_and(|n| *self.requests.lock().unwrap() >= n)
        }
    }

    fn poll_t(t: &chrono::DateTime<Utc>) -> String {
        t.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    }

    /// `n` advisories updated over the last day, one every 30 seconds.
    fn spread(n: usize, now: chrono::DateTime<Utc>) -> Vec<(usize, chrono::DateTime<Utc>)> {
        let first = now - Duration::hours(23);
        (0..n)
            .map(|i| (i, first + Duration::seconds(30 * i as i64)))
            .collect()
    }

    const NOW: &str = "2026-11-11T06:00:00Z";

    /// A team whose last full poll was a day ago: a caught-up run (10 pages).
    fn caught_up_a_day_ago(gh: &FakeGitHub) {
        gh.row.lock().unwrap().insert(
            "advisory_polled_at".into(),
            (at(NOW) - Duration::hours(23) - Duration::minutes(5)).to_rfc3339(),
        );
    }

    #[tokio::test]
    async fn a_busy_day_is_read_across_runs_without_starting_over() {
        let now = at(NOW);
        let mut gh = FakeGitHub::new(spread(25 * PER_PAGE, now));
        caught_up_a_day_ago(&gh);

        // Run 1: caught up last time, so 10 pages; the checkpoint is saved.
        gh.next_run(None, None);
        let r1 = run_advisory_poll(&mut gh, now).await;
        assert_eq!((r1.pages, r1.read), (10, 1000));
        assert!(!r1.caught_up && r1.error.is_none());
        assert_eq!(
            gh.field("advisory_poll_from"),
            poll_t(&gh.advisories[999].1)
        );
        assert_eq!(gh.field("advisory_poll_caught_up"), "false");
        assert_eq!(
            gh.field("advisory_polled_at"),
            (now - Duration::hours(23) - Duration::minutes(5)).to_rfc3339()
        );
        assert_eq!(gh.field("advisory_poll_run_at"), now.to_rfc3339());

        // Run 2 (the sweep's time is up after 10 requests): it goes on from
        // the last advisory read, never from page 1 again.
        gh.next_run(Some(10), None);
        let r2 = run_advisory_poll(&mut gh, now + Duration::hours(1)).await;
        let read = gh.read();
        assert_eq!(read.first(), Some(&999), "starts at the checkpoint");
        assert!(read.iter().all(|i| *i >= 999), "page 1 isn't read again");
        assert_eq!((r2.pages, r2.read), (10, 1000));
        assert!(!r2.caught_up);
        assert_eq!(
            gh.field("advisory_poll_from"),
            poll_t(&gh.advisories[1998].1)
        );

        // Run 3: behind, so up to 30 pages; it reaches the end.
        gh.next_run(None, None);
        let r3 = run_advisory_poll(&mut gh, now + Duration::hours(2)).await;
        assert_eq!(gh.read().first(), Some(&1998));
        assert_eq!(gh.read().last(), Some(&2499));
        assert!(r3.caught_up && r3.error.is_none());
        assert_eq!(gh.field("advisory_poll_caught_up"), "true");
        assert_eq!(
            gh.field("advisory_polled_at"),
            (now + Duration::hours(2)).to_rfc3339()
        );
        assert_eq!(gh.field("advisory_poll_from"), "");
        assert_eq!(gh.field("advisory_poll_after"), "");

        // Run 4: caught up, from the last full poll (with the overlap).
        gh.next_run(None, None);
        let r4 = run_advisory_poll(&mut gh, now + Duration::hours(3)).await;
        assert_eq!(r4.since, "2026-11-11T07:50:00Z");
        assert!(r4.caught_up);
    }

    #[tokio::test]
    async fn a_run_that_is_behind_reads_up_to_30_pages() {
        let now = at(NOW);
        let mut gh = FakeGitHub::new(spread(45 * PER_PAGE, now));
        // Never reached the end.
        gh.next_run(None, None);
        let r = run_advisory_poll(&mut gh, now).await;
        assert_eq!(r.pages, sec::ADVISORY_POLL_PAGES_BEHIND);
        assert!(!r.caught_up);
        let r = run_advisory_poll(&mut gh, now).await;
        assert!(r.caught_up, "{r:?}");
    }

    #[tokio::test]
    async fn a_block_of_one_updated_at_across_pages_does_not_loop() {
        let now = at(NOW);
        let t0 = now - Duration::hours(20);
        // 50 spread out, 2,000 (20 pages) updated in the same second, 450 more.
        let mut advs = vec![];
        for i in 0..2500 {
            let t = match i {
                0..=49 => t0 - Duration::minutes(60 - i as i64),
                50..=2049 => t0,
                _ => t0 + Duration::minutes(i as i64 - 2049),
            };
            advs.push((i, t));
        }
        let mut gh = FakeGitHub::new(advs);
        caught_up_a_day_ago(&gh);
        let mut seen = HashSet::new();
        let mut runs = 0;
        loop {
            runs += 1;
            assert!(runs <= 6, "the poll never got past the block");
            gh.next_run(Some(10), None);
            let r = run_advisory_poll(&mut gh, now).await;
            assert!(r.error.is_none(), "{r:?}");
            seen.extend(gh.read());
            if r.caught_up {
                break;
            }
        }
        assert_eq!(seen.len(), 2500, "every advisory read");
        assert_eq!(runs, 4);
    }

    #[tokio::test]
    async fn a_failed_page_keeps_the_checkpoint() {
        let now = at(NOW);
        let mut gh = FakeGitHub::new(spread(25 * PER_PAGE, now));
        caught_up_a_day_ago(&gh);
        gh.next_run(None, None);
        run_advisory_poll(&mut gh, now).await;
        let checkpoint = gh.field("advisory_poll_from");
        assert_eq!(checkpoint, poll_t(&gh.advisories[999].1));

        // Run 2: two pages, then GitHub fails.
        gh.next_run(None, Some(3));
        let r = run_advisory_poll(&mut gh, now + Duration::hours(1)).await;
        assert!(!r.caught_up);
        assert_eq!(
            r.error.as_deref(),
            Some("GitHub answered 502: Server Error")
        );
        assert_eq!(
            gh.field("advisory_poll_error"),
            "GitHub answered 502: Server Error"
        );
        assert_eq!(gh.field("advisory_poll_caught_up"), "false");
        // The two pages read before the failure count.
        let kept = gh.field("advisory_poll_from");
        assert_eq!(kept, poll_t(&gh.advisories[1198].1));

        // Run 3 starts there, and clears the error once it reaches the end.
        gh.next_run(None, None);
        let r = run_advisory_poll(&mut gh, now + Duration::hours(2)).await;
        assert_eq!(gh.read().first(), Some(&1198));
        assert!(r.caught_up);
        assert_eq!(gh.field("advisory_poll_error"), "");
    }

    #[tokio::test]
    async fn a_cursor_github_no_longer_takes_starts_that_time_again() {
        let now = at(NOW);
        let mut gh = FakeGitHub::new(spread(3 * PER_PAGE, now));
        let from = poll_t(&gh.advisories[100].1);
        gh.save_poll_fields(&[
            ("advisory_poll_from", from.clone()),
            ("advisory_poll_after", "100@another-time".into()),
        ])
        .await;
        gh.next_run(None, None);
        let r = run_advisory_poll(&mut gh, now).await;
        assert!(r.caught_up && r.error.is_none(), "{r:?}");
        assert_eq!(gh.read().first(), Some(&100));
    }
}
