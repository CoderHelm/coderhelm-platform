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
//! - **Cards**: P0 and P1 post to the team's alert channel once, then on the
//!   pull request opening, CI passing and the merge, and at half the
//!   deadline and the deadline. Every card is claimed once.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{Duration, Utc};
use common::security_fixes::{
    self as sec, Dep, Evidence, EvidenceLine, Finding, FindingState, FixUnit, Observation, Scope,
    SecurityConfig, Stage, Status, Verdict,
};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::models::{SecurityEventMessage, TicketMessage, TicketSource, TokenUsage, WorkerMessage};
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Item = HashMap<String, AttributeValue>;

/// Time one tick may spend on security work.
const TICK_BUDGET_SECS: u64 = 300;
/// Findings triaged per team per tick.
const TRIAGE_PER_TICK: usize = 6;
/// Model tokens triage may spend per team per day.
const TRIAGE_TOKENS_PER_DAY: u64 = 400_000;
/// Characters of evidence one triage question carries.
const TRIAGE_MAX_CHARS: usize = 12_000;
/// OSV records read per repository sync.
const OSV_HYDRATE_MAX: usize = 300;
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

async fn save_deps(state: &WorkerState, team_id: &str, repo: &str, deps: &[Dep]) {
    let Ok(text) = serde_json::to_string(&deps[..deps.len().min(MAX_DEPS)]) else {
        return;
    };
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&sec::deps_sk(repo)))
        .item("repo", attr_s(repo))
        .item("deps", attr_s(&text))
        .item("count", attr_n(deps.len()))
        .item("updated_at", attr_s(&Utc::now().to_rfc3339()))
        .send()
        .await
    {
        warn!(team_id, repo, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "security: could not save the dependency list");
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
    let (code, sbom) = gh
        .get_status(&format!("/repos/{owner}/{name}/dependency-graph/sbom"))
        .await?;
    if code < 300 {
        deps = sec::deps_from_sbom(&sbom);
    } else {
        info!(
            team_id,
            repo, code, "security: no dependency graph SBOM (graph off or empty)"
        );
    }
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
        apply_scopes(&mut deps, "npm", p, &sec::npm_lock_scopes(&lock));
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
                };
                if !deps.contains(&d) {
                    deps.push(d);
                }
            }
        }
    }
    save_deps(state, team_id, repo, &deps).await;
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
            sec::merges_into(f, &o.advisory.ids, &o.repo, &o.manifest, &o.package)
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

/// Fills a Dependabot observation's installed version from the inventory.
fn fill_version(o: &mut Observation, deps: &[Dep]) {
    if o.version.is_empty() {
        if let Some(v) = sec::installed_version(deps, &o.ecosystem, &o.package, &o.manifest) {
            o.version = v;
        }
    }
}

// ── Sources ─────────────────────────────────────────────────────────────

/// Dependabot's open alerts for a repository, and what access says.
async fn dependabot_alerts(gh: &GitHubClient, repo: &str) -> (Vec<Value>, &'static str) {
    let mut out = vec![];
    for page in 1..=5 {
        let path = format!("/repos/{repo}/dependabot/alerts?state=open&per_page=100&page={page}");
        let (code, body) = match gh.get_status(&path).await {
            Ok(r) => r,
            Err(_) => return (out, "on"),
        };
        match code {
            200..=299 => {
                let items = body.as_array().cloned().unwrap_or_default();
                let n = items.len();
                out.extend(items);
                if n < 100 {
                    break;
                }
            }
            403 => {
                let msg = body["message"].as_str().unwrap_or("").to_ascii_lowercase();
                return (
                    vec![],
                    if msg.contains("disabled") {
                        "off"
                    } else {
                        "no_permission"
                    },
                );
            }
            404 => return (vec![], "off"),
            _ => break,
        }
    }
    (out, "on")
}

/// OSV's records for a repository's dependencies, as observations.
async fn osv_observations(state: &WorkerState, repo: &str, deps: &[Dep]) -> Vec<Observation> {
    let lookup: Vec<&Dep> = deps.iter().filter(|d| d.ecosystem != "docker").collect();
    let mut hits: Vec<(usize, String)> = vec![];
    for (chunk_i, chunk) in lookup.chunks(500).enumerate() {
        let owned: Vec<Dep> = chunk.iter().map(|d| (*d).clone()).collect();
        let body = json!({ "queries": sec::osv_queries(&owned) });
        let resp = state
            .http
            .post(format!("{}/v1/querybatch", sec::OSV_API))
            .json(&body)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await;
        let Ok(resp) = resp else {
            warn!(repo, "security: OSV didn't answer");
            return vec![];
        };
        let Ok(v) = resp.json::<Value>().await else {
            return vec![];
        };
        for (i, r) in v["results"].as_array().into_iter().flatten().enumerate() {
            for vuln in r["vulns"].as_array().into_iter().flatten() {
                if let Some(id) = vuln["id"].as_str() {
                    hits.push((chunk_i * 500 + i, id.to_string()));
                }
            }
        }
    }
    let ids: Vec<String> = {
        let mut seen = HashSet::new();
        hits.iter()
            .filter(|(_, id)| seen.insert(id.clone()))
            .map(|(_, id)| id.clone())
            .take(OSV_HYDRATE_MAX)
            .collect()
    };
    let mut records: HashMap<String, Value> = HashMap::new();
    for id in ids {
        let url = format!("{}/v1/vulns/{}", sec::OSV_API, urlencoding::encode(&id));
        if let Ok(r) = state
            .http
            .get(url)
            .timeout(std::time::Duration::from_secs(20))
            .send()
            .await
        {
            if let Ok(v) = r.json::<Value>().await {
                records.insert(id, v);
            }
        }
    }
    let mut out = vec![];
    for (i, id) in hits {
        let (Some(d), Some(v)) = (lookup.get(i), records.get(&id)) else {
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
            ..Default::default()
        });
    }
    out
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
async fn epss(state: &WorkerState, cves: &[String]) -> HashMap<String, (f64, f64)> {
    let mut out = HashMap::new();
    for batch in sec::epss_batches(cves) {
        let url = format!("{}?cve={}", sec::EPSS_API, batch);
        let Ok(r) = state
            .http
            .get(url)
            .timeout(std::time::Duration::from_secs(20))
            .send()
            .await
        else {
            continue;
        };
        if let Ok(v) = r.json::<Value>().await {
            out.extend(sec::epss_map(&v));
        }
    }
    out
}

// ── Triage ──────────────────────────────────────────────────────────────

fn triage_key(f: &Finding) -> String {
    common::content_hash(&format!(
        "{}|{}|{:?}",
        f.advisory.ids.join(","),
        f.version,
        f.scope
    ))
}

/// The code lines that mention the search terms: code search for the
/// files, then each file read for its line numbers.
async fn code_hits(gh: &GitHubClient, repo: &str, terms: &[String]) -> (Vec<EvidenceLine>, usize) {
    let Some((owner, name)) = repo.split_once('/') else {
        return (vec![], 0);
    };
    let Ok(branch) = gh.get_default_branch(owner, name).await else {
        return (vec![], 0);
    };
    let mut files: Vec<(String, String)> = vec![];
    let mut searched = 0;
    for t in terms {
        searched += 1;
        let q = format!("\"{t}\"");
        if let Ok(results) = gh.search_code(owner, name, &branch, &q).await {
            for r in results.into_iter().take(4) {
                if is_lockfile_or_manifest(&r.path) {
                    continue;
                }
                if !files.iter().any(|(p, _)| *p == r.path) {
                    files.push((r.path, t.clone()));
                }
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
                    return (out, searched);
                }
            }
        }
    }
    (out, searched)
}

fn is_lockfile_or_manifest(path: &str) -> bool {
    sec::is_dependency_file(path) || path.ends_with(".lock") || path.ends_with(".sum")
}

/// Today's triage tokens for the team; adds `add` and returns the total.
async fn triage_tokens(state: &WorkerState, team_id: &str, add: u64) -> u64 {
    let day = Utc::now().format("%Y%m%d").to_string();
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("SECTRIAGE#{day}")))
        .update_expression("ADD tokens :n SET #ttl = :ttl")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":n", attr_n(add))
        .expression_attribute_values(":ttl", attr_n(Utc::now().timestamp() + 3 * 86_400))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
        .send()
        .await;
    r.ok()
        .and_then(|o| o.attributes().and_then(|a| a.get("tokens").cloned()))
        .and_then(|v| v.as_n().ok().and_then(|n| n.parse().ok()))
        .unwrap_or(u64::MAX)
}

/// Works out one finding's verdict. Dev-only needs no agent.
async fn triage(
    state: &WorkerState,
    team_id: &str,
    gh: &GitHubClient,
    f: &mut Finding,
    st: &mut FindingState,
) {
    st.triaged = Some(triage_key(f));
    if f.scope == Scope::Dev {
        f.verdict = Verdict::NotLoaded;
        f.evidence = Some(Evidence {
            summary: "A development dependency only (the lockfile or Dependabot says so), so it isn't loaded at runtime.".into(),
            lines: vec![],
            checked: "The dependency's scope.".into(),
            confidence: "high".into(),
        });
        return;
    }
    let terms = sec::search_terms(f, st);
    let (hits, searched) = code_hits(gh, &f.repo, &terms).await;
    let mut checked = format!(
        "Searched the code for {} ({} searches), {} matching lines.",
        terms
            .iter()
            .map(|t| format!("`{t}`"))
            .collect::<Vec<_>>()
            .join(", "),
        searched,
        hits.len()
    );
    if f.ecosystem == "Go" {
        checked.push_str(
            " govulncheck isn't available here, so the advisory's symbols were searched instead.",
        );
    }
    if let Some(why) = super::check_budget_exceeded(state, team_id).await {
        f.verdict = Verdict::Unknown;
        f.evidence = Some(Evidence {
            summary: format!("Not read by the agent: {why}."),
            lines: hits.into_iter().take(5).collect(),
            checked,
            confidence: "low".into(),
        });
        // Read again once the team has budget.
        st.triaged = None;
        return;
    }
    if triage_tokens(state, team_id, 0).await >= TRIAGE_TOKENS_PER_DAY {
        f.verdict = Verdict::Unknown;
        f.evidence = Some(Evidence {
            summary: "Not read by the agent: today's triage budget is used up. It is read again tomorrow.".into(),
            lines: hits.into_iter().take(5).collect(),
            checked,
            confidence: "low".into(),
        });
        st.triaged = None;
        return;
    }
    let provider = crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await;
    let Ok(provider) = provider else {
        f.verdict = Verdict::Unknown;
        f.evidence = Some(Evidence {
            summary: "No model is set up for this team, so only the code search ran.".into(),
            lines: hits.into_iter().take(5).collect(),
            checked,
            confidence: "low".into(),
        });
        return;
    };
    let prompt = sec::triage_prompt(f, st, &hits, TRIAGE_MAX_CHARS);
    let mut usage = TokenUsage::default();
    let answer = crate::agent::provider::converse_simple(
        state,
        &provider,
        provider.primary_model_id(),
        sec::TRIAGE_SYSTEM,
        &prompt,
        &mut usage,
    )
    .await;
    triage_tokens(state, team_id, usage.total_tokens()).await;
    match answer {
        Ok(text) => {
            let (v, mut e) = sec::parse_triage(&text, &hits);
            e.checked = checked;
            f.verdict = v;
            f.evidence = Some(e);
        }
        Err(err) => {
            warn!(team_id, finding = %f.id, error = %err, "security: triage call failed");
            st.triaged = None;
        }
    }
}

// ── Cards ───────────────────────────────────────────────────────────────

async fn team_channels(state: &WorkerState, team_id: &str) -> common::notify::Channels {
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
        let (alerts, access) = dependabot_alerts(&cx.gh, repo).await;
        set_team_state(
            cx.state,
            cx.team_id,
            &[
                ("dependabot", attr_s(access)),
                (
                    "permission_missing",
                    AttributeValue::Bool(access == "no_permission"),
                ),
            ],
        )
        .await;
        for a in &alerts {
            if let Some(mut o) = sec::observation_from_alert(repo, a) {
                fill_version(&mut o, &deps);
                obs.push(o);
            }
        }
    }
    obs.extend(osv_observations(cx.state, repo, &deps).await);
    set_team_state(
        cx.state,
        cx.team_id,
        &[("osv_synced_at", attr_s(&Utc::now().to_rfc3339()))],
    )
    .await;
    let applied = upsert(cx.state, cx.team_id, cx.cfg, findings, obs).await;
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
        let m = epss(cx.state, &cves).await;
        set_team_state(
            cx.state,
            cx.team_id,
            &[("epss_synced_at", attr_s(&now.to_rfc3339()))],
        )
        .await;
        m
    } else {
        HashMap::new()
    };
    // The most urgent first: which findings this tick triages.
    let mut wanted: Vec<(Option<sec::Tier>, usize)> = findings
        .iter()
        .enumerate()
        .filter(|(_, (f, st))| {
            f.status.is_open()
                && cx.cfg.repos.includes(&f.repo)
                && (f.verdict == Verdict::Pending
                    || st.triaged.as_deref() != Some(triage_key(f).as_str()))
        })
        .map(|(i, (f, _))| (f.tier, i))
        .collect();
    wanted.sort();
    let to_triage: HashSet<usize> = wanted
        .into_iter()
        .take(TRIAGE_PER_TICK)
        .map(|(_, i)| i)
        .collect();
    let mut triaged = 0;
    for (i, (f, st)) in findings.iter_mut().enumerate() {
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
        if to_triage.contains(&i) && !cx.out_of_time() {
            triage(cx.state, cx.team_id, &cx.gh, f, st).await;
            triaged += 1;
            if f.verdict != Verdict::Pending {
                add_event(
                    cx.state,
                    cx.team_id,
                    &f.id,
                    &format!("Verdict: {}", verdict_word(f.verdict)),
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
    report.triaged += triaged;
    track_prs(cx, findings).await;
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

/// Pull requests: merged, closed, CI; runs that ended without one.
async fn track_prs(cx: &Ctx<'_>, findings: &mut [(Finding, FindingState)]) {
    let now = Utc::now();
    let mut seen: HashMap<(String, u64), (String, Option<String>)> = HashMap::new();
    let channels = if cx.cfg.acts() {
        Some(team_channels(cx.state, cx.team_id).await)
    } else {
        None
    };
    for (f, st) in findings.iter_mut() {
        if cx.out_of_time() {
            return;
        }
        if f.status == Status::Fixing {
            // A run that ended without a pull request lets the finding open again.
            let stale =
                sec::parse_time(&f.updated_at).is_some_and(|t| now - t > Duration::hours(12));
            if let (true, Some(ticket)) = (stale, st.ticket.clone()) {
                let branch = ticket_branch(cx.state, cx.team_id, &ticket).await;
                let open = match (branch, f.repo.split_once('/')) {
                    (Some(b), Some((o, n))) => {
                        cx.gh.find_open_pr_for_branch(o, n, &b).await.ok().flatten()
                    }
                    _ => None,
                };
                match open {
                    Some(pr) => {
                        f.status = Status::PrOpen;
                        f.pr = Some(sec::PrRef {
                            number: pr["number"].as_u64().unwrap_or(0),
                            url: pr["html_url"].as_str().unwrap_or("").to_string(),
                            ci: Some("pending".into()),
                            state: "open".into(),
                            adopted: false,
                        });
                    }
                    None => {
                        f.status = Status::Open;
                        st.ticket = None;
                        st.hold_until = Some((now + Duration::days(1)).to_rfc3339());
                        add_event(
                            cx.state,
                            cx.team_id,
                            &f.id,
                            "The fix run ended without a pull request; it is tried again tomorrow",
                            None,
                        )
                        .await;
                    }
                }
                f.updated_at = now.to_rfc3339();
                save_finding(cx.state, cx.team_id, f, st).await;
            }
            continue;
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
            "merged" => {
                p.state = "merged".into();
                f.status = Status::Fixed;
                st.merged_at = Some(now.to_rfc3339());
                add_event(
                    cx.state,
                    cx.team_id,
                    &f.id,
                    &format!("PR #{} merged", p.number),
                    Some(&p.url),
                )
                .await;
            }
            "closed" => {
                p.state = "closed".into();
                f.status = Status::Open;
                st.hold_until = Some((now + Duration::days(7)).to_rfc3339());
                add_event(
                    cx.state,
                    cx.team_id,
                    &f.id,
                    &format!(
                        "PR #{} was closed without merging; not opened again for 7 days",
                        p.number
                    ),
                    Some(&p.url),
                )
                .await;
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
            }
        }
        f.pr = Some(p.clone());
        if *f != before {
            f.updated_at = now.to_rfc3339();
            save_finding(cx.state, cx.team_id, f, st).await;
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
    let ci = match gh.list_check_runs_for_ref(o, n, sha).await {
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

/// New cards and escalations.
async fn cards(cx: &Ctx<'_>, findings: &mut [(Finding, FindingState)]) -> usize {
    let channels = team_channels(cx.state, cx.team_id).await;
    if channels.is_empty() {
        return 0;
    }
    let now = Utc::now();
    let mut posted = 0;
    for (f, st) in findings.iter_mut() {
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
        let Some(stage) = stage else { continue };
        if post_card(cx.state, cx.team_id, &channels, &f.id, stage, f).await {
            st.cards.push(stage.key().to_string());
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
    }
}

/// Fix runs (or adoptions) for the units due now.
async fn start_fixes(cx: &Ctx<'_>, findings: &mut [(Finding, FindingState)]) -> usize {
    let now = Utc::now();
    let units = sec::plan_units(findings, cx.cfg, now);
    let mut started = 0;
    let mut open_pulls: HashMap<String, Vec<Value>> = HashMap::new();
    for unit in units {
        if cx.out_of_time() || !sec::unit_due(&unit, cx.cfg) {
            continue;
        }
        if unit.grouped {
            let sk = format!("SEC_BATCH#{}#{}", unit.repo.to_ascii_lowercase(), unit.key);
            if !claim(cx.state, cx.team_id, &sk, 8 * 86_400).await {
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
            continue;
        }
        let members: Vec<usize> = unit
            .finding_ids
            .iter()
            .filter_map(|id| findings.iter().position(|(f, _)| &f.id == id))
            .collect();
        // An open Dependabot or Renovate pull request for the package.
        if cx.cfg.adopt_bot_prs && !unit.grouped {
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
            if let Some(pr) = bot_pr_for(&pulls, &unit) {
                adopt(cx, &unit, &pr, findings, &members).await;
                started += 1;
                continue;
            }
        }
        let major = unit.bumps.iter().any(|b| b.major);
        if major && cx.cfg.major_bumps == sec::MajorBumps::IssueOnly {
            issue_only(cx, &unit, findings, &members).await;
            continue;
        }
        if let Some(why) = super::check_budget_exceeded(cx.state, cx.team_id).await {
            info!(team_id = cx.team_id, ticket = %unit.ticket, why, "security: fix run not started (budget)");
            release(cx.state, cx.team_id, &format!("SEC_RUN#{}", unit.ticket)).await;
            continue;
        }
        match enqueue(cx, &unit, findings, &members).await {
            Ok(()) => started += 1,
            Err(e) => {
                warn!(team_id = cx.team_id, ticket = %unit.ticket, error = %e, "security: could not start the fix run");
                release(cx.state, cx.team_id, &format!("SEC_RUN#{}", unit.ticket)).await;
            }
        }
    }
    started
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
    if let Err(e) = cx.gh.add_labels(o, n, number, &unit.labels).await {
        warn!(repo = %unit.repo, number, error = %e, "security: could not label the adopted pull request");
    }
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
        });
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
    let body = sec::ticket_body(unit, &member_refs, cx.cfg);
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
        st.ticket = Some(unit.ticket.clone());
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

async fn team_tick(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    cfg: &SecurityConfig,
    kev_map: &BTreeMap<String, String>,
    started: Instant,
) -> Result<TeamReport, Error> {
    let cx = Ctx {
        state,
        team_id,
        cfg,
        gh: github(state, installation_id)?,
        started,
    };
    let mut report = TeamReport::default();
    let mut findings = load_findings(state, team_id).await;
    let day = Utc::now().format("%Y%m%d").to_string();
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
            continue;
        }
        report.new_findings += sync_repo(&cx, &repo, &mut findings, true).await;
        report.repos_synced += 1;
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
    pub repos_synced: usize,
    pub new_findings: usize,
    pub triaged: usize,
    pub runs: usize,
    pub cards: usize,
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
        if started.elapsed().as_secs() >= TICK_BUDGET_SECS {
            break;
        }
        let Some(cfg) = load_config(state, &team).await else {
            continue;
        };
        if !cfg.enabled {
            continue;
        }
        let Some(iid) = super::review_reminder::team_installation_id(state, &team).await else {
            continue;
        };
        if kev_map.is_none() {
            kev_map = Some(kev(state).await);
        }
        let kev_ref = kev_map.as_ref().expect("set above");
        report.teams += 1;
        match team_tick(state, &team, iid, &cfg, kev_ref, started).await {
            Ok(r) => {
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
    let kev_map = kev(state).await;
    for team in teams {
        let Some(cfg) = load_config(state, &team).await.filter(|c| c.enabled) else {
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
                // Every finding that names it, whatever its version now.
                let ghsa = msg.ghsa_id.to_ascii_uppercase();
                for (f, _) in findings.iter() {
                    if f.advisory.ids.iter().any(|i| i.eq_ignore_ascii_case(&ghsa)) {
                        obs.push(Observation {
                            repo: f.repo.clone(),
                            manifest: f.manifest.clone(),
                            ecosystem: f.ecosystem.clone(),
                            package: f.package.clone(),
                            advisory: sec::AdvisoryRecord {
                                ids: vec![msg.ghsa_id.clone()],
                                withdrawn: true,
                                source: "github_advisory".into(),
                                ..Default::default()
                            },
                            ..Default::default()
                        });
                    }
                }
            }
            upsert(state, team_id, cfg, &mut findings, obs).await;
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
            for repo in repos {
                if cx.out_of_time() {
                    break;
                }
                report.new_findings += sync_repo(&cx, &repo, &mut findings, true).await;
            }
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
    let members = unit_findings(state, &msg.team_id, &unit).await;
    let pairs: Vec<(&Finding, &FindingState)> = members.iter().map(|(f, s)| (f, s)).collect();
    Some(sec::pr_body(&unit, &pairs, change))
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
    let mut members = unit_findings(state, &msg.team_id, &unit).await;
    let cfg = load_config(state, &msg.team_id).await.unwrap_or_default();
    let channels = if cfg.acts() {
        team_channels(state, &msg.team_id).await
    } else {
        Default::default()
    };
    let mut carded = false;
    for (f, st) in members.iter_mut() {
        f.status = Status::PrOpen;
        f.pr = Some(sec::PrRef {
            number,
            url: url.to_string(),
            ci: Some("pending".into()),
            state: "open".into(),
            adopted: false,
        });
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
            },
            Dep {
                ecosystem: "PyPI".into(),
                name: "Django".into(),
                version: "4.2".into(),
                manifest: sec::SBOM_MANIFEST.into(),
                scope: Scope::Unknown,
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
}
