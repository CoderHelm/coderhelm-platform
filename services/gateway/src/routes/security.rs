//! Security fixes from the dashboard and from GitHub's webhooks
//! (`common::security_fixes` has the rules, `worker::passes::security_fixes`
//! the work).
//!
//! - `GET /api/security/overview` — the counts, the sources, the settings'
//!   switches. Any team member.
//! - `GET /api/security/findings?status=&tier=&repo=&cursor=` — the findings.
//! - `GET /api/security/findings/{id}` — one finding and its timeline.
//! - `POST /api/security/findings/{id}/dismiss` — with a reason and an
//!   optional end date; synced to the Dependabot alert when the app may.
//!   Admin.
//! - `POST /api/security/findings/{id}/reopen` — Admin.
//! - `POST /api/security/scan` — check one repository (or all) now. Admin.
//! - `GET /api/security/config`, `PUT /api/security/config` — the settings
//!   and the defaults; saving is Admin.
//! - `GET /api/security/preview` — what a dry run would open and post now.
//!   Admin.
//! - The `dependabot_alert` and `security_advisory` webhooks, and a push to
//!   a default branch that touches a dependency file, queue a
//!   `security_event` for the worker when the team turned this on.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Path, Query, State};
use axum::{http::StatusCode, Extension, Json};
use common::security_fixes::{
    self as sec, Dismissed, Finding, FindingState, SecurityConfig, Status, Tier,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::{Claims, SecurityEventMessage, WorkerMessage};
use crate::AppState;

type Item = HashMap<String, AttributeValue>;

/// Findings shown per page.
const PAGE: usize = 50;
const MAX_FINDINGS: usize = 5_000;

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn s(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn bad(msg: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg })))
}

fn code(c: StatusCode) -> (StatusCode, Json<Value>) {
    (
        c,
        Json(json!({ "error": c.canonical_reason().unwrap_or("error") })),
    )
}

async fn get_item(state: &AppState, pk: &str, sk: &str) -> Result<Option<Item>, StatusCode> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(pk))
        .key("sk", attr_s(sk))
        .send()
        .await
        .map(|o| o.item().cloned())
        .map_err(|e| {
            error!(sk, error = %e, "security: read failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// The team's settings: saved, or the defaults.
pub(crate) async fn load_config(
    state: &AppState,
    team_id: &str,
) -> Result<Option<SecurityConfig>, StatusCode> {
    Ok(get_item(state, team_id, sec::CONFIG_SK)
        .await?
        .and_then(|i| SecurityConfig::from_json(&s(&i, "config"))))
}

async fn load_findings(
    state: &AppState,
    team_id: &str,
) -> Result<Vec<(Finding, FindingState)>, StatusCode> {
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
            .await
            .map_err(|e| {
                error!(error = %e, "security: could not read the findings");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        for i in resp.items() {
            if let Ok(f) = serde_json::from_str::<Finding>(&s(i, "finding")) {
                let st = serde_json::from_str(&s(i, "state")).unwrap_or_default();
                out.push((f, st));
            }
        }
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() && out.len() < MAX_FINDINGS => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

async fn load_finding(
    state: &AppState,
    team_id: &str,
    id: &str,
) -> Result<(Finding, FindingState), StatusCode> {
    if id.is_empty() || id.len() > 64 || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(StatusCode::NOT_FOUND);
    }
    let item = get_item(state, team_id, &sec::finding_sk(id))
        .await?
        .ok_or(StatusCode::NOT_FOUND)?;
    let f =
        serde_json::from_str::<Finding>(&s(&item, "finding")).map_err(|_| StatusCode::NOT_FOUND)?;
    let st = serde_json::from_str(&s(&item, "state")).unwrap_or_default();
    Ok((f, st))
}

async fn save_finding(
    state: &AppState,
    team_id: &str,
    f: &Finding,
    st: &FindingState,
) -> Result<(), StatusCode> {
    let fj = serde_json::to_string(f).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let sj = serde_json::to_string(st).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
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
        .await
        .map(|_| ())
        .map_err(|e| {
            error!(error = %e, "security: could not save a finding");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn add_event(state: &AppState, team_id: &str, id: &str, text: &str, link: Option<&str>) {
    let now = chrono::Utc::now();
    let n = now.timestamp_subsec_micros() % 1000;
    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&sec::event_sk(id, &now, n)))
        .item("at", attr_s(&now.to_rfc3339()))
        .item("text", attr_s(common::truncate_str(text, 500)))
        .item(
            "ttl",
            AttributeValue::N((now.timestamp() + sec::EVENT_TTL_DAYS * 86_400).to_string()),
        );
    if let Some(l) = link {
        put = put.item("link", attr_s(l));
    }
    if let Err(e) = put.send().await {
        warn!(error = %e, "security: could not record a timeline line");
    }
}

// ── Overview ────────────────────────────────────────────────────────────

/// The items of one partition whose sort key starts with `prefix`, only the
/// named attributes.
async fn query_prefix(
    state: &AppState,
    pk: &str,
    prefix: &str,
    attrs: &[&str],
) -> Result<Vec<Item>, StatusCode> {
    let mut out = vec![];
    let mut start: Option<Item> = None;
    let names: Vec<String> = (0..attrs.len()).map(|i| format!("#a{i}")).collect();
    for _ in 0..20 {
        let mut q = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
            .expression_attribute_values(":p", attr_s(pk))
            .expression_attribute_values(":pfx", attr_s(prefix))
            .projection_expression(names.join(", "))
            .set_exclusive_start_key(start.take());
        for (n, a) in names.iter().zip(attrs) {
            q = q.expression_attribute_names(n, *a);
        }
        let resp = q.send().await.map_err(|e| {
            error!(prefix, error = %e, "security: read failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        out.extend(resp.items().iter().cloned());
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

fn num(i: &Item, k: &str) -> u64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<f64>().ok())
        .map(|f| f.max(0.0) as u64)
        .unwrap_or(0)
}

/// The later of two RFC 3339 times (either may be missing).
fn latest(a: Option<String>, b: Option<String>) -> Option<String> {
    let at = |t: &Option<String>| {
        t.as_deref()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
    };
    match (at(&a), at(&b)) {
        (Some(x), Some(y)) if y > x => b,
        (Some(_), _) => a,
        (None, Some(_)) => b,
        (None, None) => a.or(b),
    }
}

/// What the advisory feed row says: nothing once a webhook arrived; until
/// then, that the poll is what keeps it current.
fn advisory_feed_reason(webhook_at: Option<&str>, polled_at: Option<&str>) -> String {
    match (webhook_at, polled_at) {
        (Some(_), _) => String::new(),
        (None, Some(p)) => {
            let shown = chrono::DateTime::parse_from_rfc3339(p)
                .map(|t| {
                    t.with_timezone(&chrono::Utc)
                        .format("%b %-d, %H:%M UTC")
                        .to_string()
                })
                .unwrap_or_else(|_| p.to_string());
            format!("Waiting for GitHub's first webhook; checked by polling at {shown}.")
        }
        (None, None) => "Waiting for GitHub's first webhook; GitHub's advisory database is also polled about hourly, starting on the next check.".into(),
    }
}

/// One row per source: what it gives, its state in plain words, when it
/// last synced, and how many findings came from it.
pub fn source_rows(team_state: &Item, kev: &Item, findings: &[(Finding, FindingState)]) -> Value {
    let opt = |i: &Item, k: &str| Some(s(i, k)).filter(|v| !v.is_empty());
    let open: Vec<&Finding> = findings
        .iter()
        .map(|(f, _)| f)
        .filter(|f| f.status.is_open())
        .collect();
    let from = |src: &str| open.iter().filter(|f| f.advisory.source == src).count();
    let dependabot_state = s(team_state, "dependabot");
    let (d_state, d_reason) = match dependabot_state.as_str() {
        "" => ("never", "Read on the first check of each repository.".to_string()),
        "on" => ("ok", String::new()),
        "off" => ("off", opt(team_state, "dependabot_reason").unwrap_or_else(|| "Dependabot alerts are turned off for a repository; turn them on in its Settings, Code security.".into())),
        "not_accepted" | "no_permission" => ("needs_permission", opt(team_state, "dependabot_reason").unwrap_or_else(|| "The GitHub App needs Dependabot alerts: read.".into())),
        _ => ("error", opt(team_state, "dependabot_reason").unwrap_or_else(|| "The last read of Dependabot's alerts failed; the next check records why.".into())),
    };
    let osv_error = opt(team_state, "osv_error");
    let osv_synced = opt(team_state, "osv_synced_at");
    let adv_at = opt(team_state, "advisory_event_at");
    let adv_polled = opt(team_state, "advisory_polled_at");
    let kev_at = opt(kev, "synced_at");
    let epss_at = opt(team_state, "epss_synced_at");
    json!([
        {
            "id": "dependabot",
            "name": "Dependabot alerts",
            "gives": "GitHub's own alerts for each repository's dependencies, with scope and the dependency path.",
            "state": d_state,
            "access": if dependabot_state.is_empty() { "on" } else { dependabot_state.as_str() },
            "reason": d_reason,
            "synced_at": opt(team_state, "dependabot_checked_at"),
            "count": open.iter().filter(|f| f.dependabot_alert.is_some() || f.advisory.source == "dependabot").count(),
        },
        {
            "id": "osv",
            "name": "OSV (by API)",
            "gives": "Open Source Vulnerabilities: every package and version in the dependency lists, matched against advisories from GitHub, PyPI, Go, RustSec and more.",
            "state": match (&osv_error, &osv_synced) {
                (Some(_), _) => "error",
                (None, Some(_)) => "ok",
                _ => "never",
            },
            "reason": osv_error.clone().map(|e| format!("{e}; tried again on the next check.")).unwrap_or_default(),
            "synced_at": osv_synced,
            "count": from("osv"),
        },
        {
            "id": "github_advisory",
            "name": "GitHub advisory feed",
            "gives": "New and changed GitHub advisories (the security_advisory webhook, and GitHub's advisory database read about hourly), matched against the dependency lists.",
            "state": if adv_at.is_some() || adv_polled.is_some() { "ok" } else { "never" },
            "reason": advisory_feed_reason(adv_at.as_deref(), adv_polled.as_deref()),
            "synced_at": latest(adv_at.clone(), adv_polled.clone()),
            "webhook_at": adv_at,
            "polled_at": adv_polled,
            "count": from("github_advisory"),
        },
        {
            "id": "kev",
            "name": "CISA known exploited list",
            "gives": "Vulnerabilities attackers are using now, with CISA's due date. On the list and loaded at runtime is P0.",
            "state": if kev_at.is_some() { "ok" } else { "never" },
            "reason": if kev_at.is_some() { String::new() } else { "Read once a day on the first check.".into() },
            "synced_at": kev_at,
            "count": open.iter().filter(|f| f.advisory.kev).count(),
            "listed": num(kev, "count"),
        },
        {
            "id": "epss",
            "name": "FIRST EPSS",
            "gives": "The chance each vulnerability is exploited in the next 30 days. 0.1 or more raises the tier.",
            "state": if epss_at.is_some() { "ok" } else { "never" },
            "reason": if epss_at.is_some() { String::new() } else { "Read once a day for open findings.".into() },
            "synced_at": epss_at,
            "count": open.iter().filter(|f| f.advisory.epss.is_some()).count(),
        },
    ])
}

/// The dependency inventory per repository, and the reason a repository's
/// couldn't be read.
/// When a read dependency list is read again (a push to the default branch
/// that changes a dependency file queues a read; the daily check is the
/// backstop).
pub const NEXT_READ: &str = "Next read on the next dependency change or within 24 h.";

/// `team_state`: the team's row, whose Dependabot state a repository's is
/// shown only when it differs from.
pub fn repo_rows(deps: &[Item], cfg: &SecurityConfig, team_state: &Item) -> Value {
    let team_dependabot = (
        s(team_state, "dependabot"),
        s(team_state, "dependabot_reason"),
    );
    let mut rows: Vec<Value> = deps
        .iter()
        .map(|i| {
            let ecos: BTreeMap<String, u64> =
                serde_json::from_str(&s(i, "ecosystems")).unwrap_or_default();
            let state = Some(s(i, "inventory_state"))
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| "ok".into());
            let read_at = Some(s(i, "read_at"))
                .filter(|v| !v.is_empty())
                .or_else(|| Some(s(i, "updated_at")).filter(|v| !v.is_empty()));
            let own = (s(i, "dependabot"), s(i, "dependabot_reason"));
            let (dependabot, dependabot_reason) = if own.0.is_empty() || own == team_dependabot {
                (None, String::new())
            } else {
                (Some(own.0), own.1)
            };
            json!({
                "repo": s(i, "repo"),
                "packages": num(i, "count"),
                "ecosystems": ecos,
                "next_read": if read_at.is_some() { NEXT_READ } else { "" },
                "read_at": read_at,
                "state": state,
                "reason": s(i, "inventory_reason"),
                "unpinned": num(i, "unpinned"),
                "dependabot": dependabot,
                "dependabot_reason": dependabot_reason,
                "osv_error": Some(s(i, "osv_error")).filter(|v| !v.is_empty()),
            })
        })
        .collect();
    if let sec::Repos::List(l) = &cfg.repos {
        for r in l {
            if !rows.iter().any(|x| {
                x["repo"]
                    .as_str()
                    .is_some_and(|x| x.eq_ignore_ascii_case(r))
            }) {
                rows.push(json!({
                    "repo": r, "packages": 0, "ecosystems": {}, "read_at": null, "state": "not_read",
                    "reason": "Not read yet: its first check runs within 20 minutes of turning Security on.",
                    "unpinned": 0, "dependabot": null, "dependabot_reason": "", "osv_error": null, "next_read": "",
                }));
            }
        }
    }
    rows.sort_by(|a, b| {
        a["repo"]
            .as_str()
            .unwrap_or("")
            .to_ascii_lowercase()
            .cmp(&b["repo"].as_str().unwrap_or("").to_ascii_lowercase())
    });
    Value::Array(rows)
}

/// Tokens spent by one feature in an analytics period item.
async fn feature_tokens(state: &AppState, team_id: &str, period: &str, feature: &str) -> u64 {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key("team_id", attr_s(team_id))
        .key("period", attr_s(period))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned());
    common::usage::feature_totals(item.as_ref())
        .into_iter()
        .find(|(f, _)| f == feature)
        .map(|(_, u)| u.total())
        .unwrap_or(0)
}

/// Today's and this month's spend and the day's limits.
async fn spend(state: &AppState, team_id: &str, cfg: &SecurityConfig) -> Result<Value, StatusCode> {
    let now = chrono::Utc::now();
    let day = now.format("%Y%m%d").to_string();
    let month = now.format("%Y%m").to_string();
    let triage = query_prefix(
        state,
        team_id,
        &format!("SECTRIAGE#{month}"),
        &["sk", "tokens", "packages"],
    )
    .await?;
    let today = triage
        .iter()
        .find(|i| s(i, "sk") == format!("SECTRIAGE#{day}"));
    let runs = get_item(state, team_id, &format!("SECRUNS#{day}"))
        .await?
        .unwrap_or_default();
    let cards = get_item(state, team_id, &format!("SECCARDS#{day}"))
        .await?
        .unwrap_or_default();
    let fix_day = feature_tokens(
        state,
        team_id,
        &format!("DAY#{}", now.format("%Y-%m-%d")),
        common::usage::feature::SECURITY_FIX,
    )
    .await;
    let fix_month = feature_tokens(
        state,
        team_id,
        &now.format("%Y-%m").to_string(),
        common::usage::feature::SECURITY_FIX,
    )
    .await;
    let triage_month: u64 = triage.iter().map(|i| num(i, "tokens")).sum();
    Ok(json!({
        "triage": {
            "packages_today": today.map(|i| num(i, "packages")).unwrap_or(0),
            "tokens_today": today.map(|i| num(i, "tokens")).unwrap_or(0),
            "tokens_limit": cfg.triage_tokens_per_day,
            "tokens_month": triage_month,
        },
        "fix": {
            "runs_today": num(&runs, "runs"),
            "runs_limit": cfg.max_fix_runs_per_day,
            "tokens_today": fix_day,
            "tokens_limit": cfg.max_fix_tokens_per_day,
            "tokens_month": fix_month,
        },
        "cards": {
            "today": num(&cards, "cards"),
            "limit": cfg.max_cards_per_day,
        },
        "tokens_month": triage_month + fix_month,
    }))
}

/// GET /api/security/overview
pub async fn overview(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let cfg = load_config(&state, &claims.team_id)
        .await?
        .unwrap_or_default();
    let findings = load_findings(&state, &claims.team_id).await?;
    let now = chrono::Utc::now();
    let mut out = sec::overview_counts(&findings, now);
    let team_state = get_item(&state, &claims.team_id, sec::STATE_SK)
        .await?
        .unwrap_or_default();
    let kev = get_item(&state, sec::GLOBAL_PK, sec::KEV_SK)
        .await?
        .unwrap_or_default();
    let opt = |i: &Item, k: &str| Some(s(i, k)).filter(|v| !v.is_empty());
    let dependabot = match s(&team_state, "dependabot").as_str() {
        "off" => "off",
        "no_permission" => "no_permission",
        "not_accepted" => "not_accepted",
        "error" => "error",
        // Not read yet: "on" until a read says otherwise.
        _ => "on",
    };
    let permission_missing = matches!(dependabot, "no_permission" | "not_accepted");
    out["enabled"] = json!(cfg.enabled);
    out["dry_run"] = json!(cfg.dry_run);
    out["bundle"] = json!(cfg.bundle);
    out["sources"] = json!({
        "dependabot": dependabot,
        "dependabot_reason": opt(&team_state, "dependabot_reason"),
        "osv": { "synced_at": opt(&team_state, "osv_synced_at"), "mode": "api" },
        "kev_synced_at": opt(&kev, "synced_at"),
        "epss_synced_at": opt(&team_state, "epss_synced_at"),
        "repository_advisories": sec::repo_advisories_source(&s(&team_state, "repository_advisories")),
    });
    out["source_rows"] = source_rows(&team_state, &kev, &findings);
    let deps = query_prefix(
        &state,
        &claims.team_id,
        sec::DEPS_PREFIX,
        &[
            "repo",
            "count",
            "ecosystems",
            "read_at",
            "updated_at",
            "inventory_state",
            "inventory_reason",
            "unpinned",
            "dependabot",
            "dependabot_reason",
            "osv_error",
        ],
    )
    .await?;
    out["repos"] = repo_rows(&deps, &cfg, &team_state);
    out["spend"] = spend(&state, &claims.team_id, &cfg).await?;
    out["permission_missing"] = json!(permission_missing);
    Ok(Json(out))
}

// ── Groups ──────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct GroupQuery {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub repo: Option<String>,
    /// Matches the package, an advisory id or title.
    #[serde(default)]
    pub q: Option<String>,
    /// `tier` (default), `due`, `package`.
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Pure: the groups a query shows, in order, and the next page's cursor.
pub fn group_page(
    findings: &[(Finding, FindingState)],
    q: &GroupQuery,
) -> Result<(Vec<sec::Group>, usize, Option<String>), StatusCode> {
    let status = q.status.as_deref().unwrap_or("open");
    if !matches!(status, "open" | "fixed" | "dismissed" | "all") {
        return Err(StatusCode::BAD_REQUEST);
    }
    let tier = match q.tier.as_deref().filter(|t| !t.is_empty()) {
        Some(t) => Some(Tier::parse(t).ok_or(StatusCode::BAD_REQUEST)?),
        None => None,
    };
    let repo = q.repo.as_deref().filter(|r| !r.is_empty());
    if repo.is_some_and(|r| !sec::valid_repo(r)) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let offset: usize = match q.cursor.as_deref().filter(|c| !c.is_empty()) {
        Some(c) => c.parse().map_err(|_| StatusCode::BAD_REQUEST)?,
        None => 0,
    };
    let needle =
        q.q.as_deref()
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty());
    let mut list: Vec<sec::Group> = sec::groups(findings)
        .into_iter()
        .filter(|g| match status {
            "open" => g.status.is_open(),
            "fixed" => g.status == Status::Fixed,
            "dismissed" => matches!(g.status, Status::Dismissed | Status::Withdrawn),
            _ => true,
        })
        .filter(|g| tier.is_none_or(|t| g.tier == Some(t)))
        .filter(|g| repo.is_none_or(|r| g.repo.eq_ignore_ascii_case(r)))
        .filter(|g| {
            needle.as_ref().is_none_or(|n| {
                g.package.to_ascii_lowercase().contains(n)
                    || g.advisories.iter().any(|a| {
                        a.title.to_ascii_lowercase().contains(n)
                            || a.ids.iter().any(|i| i.to_ascii_lowercase().contains(n))
                    })
            })
        })
        .collect();
    match q.sort.as_deref().unwrap_or("tier") {
        "tier" => {}
        "due" => list.sort_by(|a, b| a.due_at.cmp(&b.due_at).then_with(|| a.tier.cmp(&b.tier))),
        "package" => list.sort_by(|a, b| {
            a.package
                .to_ascii_lowercase()
                .cmp(&b.package.to_ascii_lowercase())
                .then_with(|| a.version.cmp(&b.version))
        }),
        _ => return Err(StatusCode::BAD_REQUEST),
    }
    let total = list.len();
    let page: Vec<sec::Group> = list.into_iter().skip(offset).take(PAGE).collect();
    let next = (offset + PAGE < total).then(|| (offset + PAGE).to_string());
    Ok((page, total, next))
}

/// GET /api/security/groups: one row per fix (a package at one version),
/// with every advisory it clears.
pub async fn list_groups(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<GroupQuery>,
) -> Result<Json<Value>, StatusCode> {
    let findings = load_findings(&state, &claims.team_id).await?;
    let (page, total, next) = group_page(&findings, &q)?;
    let advisories: usize = page.iter().map(|g| g.advisories.len()).sum();
    let waiting: HashMap<String, Option<String>> = findings
        .iter()
        .map(|(f, _)| (f.id.clone(), f.waiting.clone()))
        .collect();
    let rows: Vec<Value> = page
        .iter()
        .map(|g| {
            let mut v = serde_json::to_value(g).unwrap_or_default();
            v["waiting"] = json!(g
                .finding_ids
                .iter()
                .find_map(|id| waiting.get(id).cloned().flatten()));
            v["evidence"] = json!(findings
                .iter()
                .find(|(f, _)| g.finding_ids.first() == Some(&f.id))
                .and_then(|(f, _)| f.evidence.clone()));
            v
        })
        .collect();
    Ok(Json(
        json!({ "groups": rows, "total": total, "advisories_on_page": advisories, "next_cursor": next }),
    ))
}

// ── Insights ────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct InsightsQuery {
    #[serde(default)]
    pub days: Option<i64>,
}

/// GET /api/security/insights?days=30: Analytics' security section.
pub async fn insights(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<InsightsQuery>,
) -> Result<Json<Value>, StatusCode> {
    let days = q.days.unwrap_or(30).clamp(7, 365);
    let findings = load_findings(&state, &claims.team_id).await?;
    let now = chrono::Utc::now();
    let from = (now - chrono::Duration::days(days))
        .format("%Y%m%d")
        .to_string();
    let triage = query_prefix(&state, &claims.team_id, "SECTRIAGE#", &["sk", "tokens"]).await?;
    let mut spend: BTreeMap<String, u64> = BTreeMap::new();
    for i in &triage {
        let d = s(i, "sk").trim_start_matches("SECTRIAGE#").to_string();
        if d.len() == 8 && d >= from {
            spend.insert(
                format!("{}-{}-{}", &d[..4], &d[4..6], &d[6..8]),
                num(i, "tokens"),
            );
        }
    }
    let mut out = sec::insights(&findings, &spend, now, days);
    out["fix_tokens_month"] = json!(
        feature_tokens(
            &state,
            &claims.team_id,
            &now.format("%Y-%m").to_string(),
            common::usage::feature::SECURITY_FIX
        )
        .await
    );
    out["enabled"] = json!(load_config(&state, &claims.team_id)
        .await?
        .is_some_and(|c| c.enabled));
    Ok(Json(out))
}

// ── Findings ────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct ListQuery {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Pure: the findings a list query shows, in order (tier, then due date),
/// and the cursor of the next page.
pub fn list_page(
    findings: Vec<Finding>,
    q: &ListQuery,
) -> Result<(Vec<Finding>, Option<String>), StatusCode> {
    let status = q.status.as_deref().unwrap_or("open");
    if !matches!(status, "open" | "fixed" | "dismissed" | "all") {
        return Err(StatusCode::BAD_REQUEST);
    }
    let tier = match q.tier.as_deref().filter(|t| !t.is_empty()) {
        Some(t) => Some(Tier::parse(t).ok_or(StatusCode::BAD_REQUEST)?),
        None => None,
    };
    let repo = q.repo.as_deref().filter(|r| !r.is_empty());
    if repo.is_some_and(|r| !sec::valid_repo(r)) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let offset: usize = match q.cursor.as_deref().filter(|c| !c.is_empty()) {
        Some(c) => c.parse().map_err(|_| StatusCode::BAD_REQUEST)?,
        None => 0,
    };
    let mut list: Vec<Finding> = findings
        .into_iter()
        .filter(|f| match status {
            "open" => f.status.is_open(),
            "fixed" => f.status == Status::Fixed,
            "dismissed" => matches!(f.status, Status::Dismissed | Status::Withdrawn),
            _ => true,
        })
        .filter(|f| tier.is_none_or(|t| f.tier == Some(t)))
        .filter(|f| repo.is_none_or(|r| f.repo.eq_ignore_ascii_case(r)))
        .collect();
    list.sort_by(|a, b| {
        a.tier
            .unwrap_or(Tier::P3)
            .cmp(&b.tier.unwrap_or(Tier::P3))
            .then_with(|| a.due_at.cmp(&b.due_at))
            .then_with(|| a.id.cmp(&b.id))
    });
    let total = list.len();
    let page: Vec<Finding> = list.into_iter().skip(offset).take(PAGE).collect();
    let next = (offset + PAGE < total).then(|| (offset + PAGE).to_string());
    Ok((page, next))
}

/// GET /api/security/findings
pub async fn list_findings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Value>, StatusCode> {
    let findings = load_findings(&state, &claims.team_id).await?;
    let (page, next) = list_page(findings.into_iter().map(|(f, _)| f).collect(), &q)?;
    Ok(Json(json!({ "findings": page, "next_cursor": next })))
}

/// GET /api/security/findings/{id}
pub async fn get_finding(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let (f, _) = load_finding(&state, &claims.team_id, &id).await?;
    let mut timeline = vec![];
    let mut start: Option<Item> = None;
    for _ in 0..10 {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
            .expression_attribute_values(":p", attr_s(&claims.team_id))
            .expression_attribute_values(":pfx", attr_s(&sec::event_prefix(&f.id)))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| {
                error!(error = %e, "security: could not read the timeline");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        for i in resp.items() {
            let link = s(i, "link");
            timeline.push(json!({
                "at": s(i, "at"),
                "text": s(i, "text"),
                "link": if link.is_empty() { Value::Null } else { json!(link) },
            }));
        }
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(Json(json!({ "finding": f, "timeline": timeline })))
}

#[derive(Deserialize)]
pub struct DismissBody {
    pub reason: String,
    #[serde(default)]
    pub until: Option<String>,
}

/// Pure: a dismissal's reason and end date, checked.
pub fn check_dismiss(
    b: &DismissBody,
    today: chrono::NaiveDate,
) -> Result<(String, Option<String>), &'static str> {
    let reason = b.reason.trim();
    if reason.is_empty() {
        return Err("a reason is required");
    }
    if reason.chars().count() > 500 {
        return Err("the reason is at most 500 characters");
    }
    let until = match b.until.as_deref().map(str::trim).filter(|u| !u.is_empty()) {
        None => None,
        Some(u) => {
            let d = chrono::NaiveDate::parse_from_str(u.get(..10).unwrap_or(u), "%Y-%m-%d")
                .map_err(|_| "until is a date (YYYY-MM-DD)")?;
            if d < today {
                return Err("until is in the past");
            }
            Some(d.format("%Y-%m-%d").to_string())
        }
    };
    Ok((reason.to_string(), until))
}

/// GitHub's reason word for a dismissal, from the verdict.
pub fn dismissed_reason(f: &Finding) -> &'static str {
    match f.verdict {
        sec::Verdict::NotReachable | sec::Verdict::NotLoaded => "not_used",
        _ => "tolerable_risk",
    }
}

/// Changes the finding's Dependabot alert; whether GitHub took it. A refusal
/// (no write permission) leaves it recorded in CoderHelm only.
async fn sync_alert(state: &AppState, team_id: &str, f: &Finding, body: &Value) -> bool {
    let Some(alert) = &f.dependabot_alert else {
        return false;
    };
    let Ok(iid) = super::api::get_team_installation_id(state, team_id).await else {
        return false;
    };
    let Ok(token) = crate::auth::github_app::get_installation_token(state, iid).await else {
        return false;
    };
    let url = format!(
        "https://api.github.com/repos/{}/dependabot/alerts/{}",
        f.repo, alert.number
    );
    match state
        .http
        .patch(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .json(body)
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => true,
        Ok(r) => {
            info!(repo = %f.repo, alert = alert.number, status = r.status().as_u16(), "security: the Dependabot alert wasn't changed (recorded in CoderHelm only)");
            false
        }
        Err(e) => {
            warn!(repo = %f.repo, error = %e, "security: could not reach GitHub for the Dependabot alert");
            false
        }
    }
}

/// POST /api/security/findings/{id}/dismiss
pub async fn dismiss(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<DismissBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    claims.require_role(3).map_err(code)?;
    let (reason, until) = check_dismiss(&body, chrono::Utc::now().date_naive()).map_err(bad)?;
    let (mut f, st) = load_finding(&state, &claims.team_id, &id)
        .await
        .map_err(code)?;
    let now = chrono::Utc::now().to_rfc3339();
    f.status = Status::Dismissed;
    f.dismissed = Some(Dismissed {
        by: claims.email.clone(),
        reason: reason.clone(),
        until: until.clone(),
        at: now.clone(),
    });
    f.updated_at = now;
    save_finding(&state, &claims.team_id, &f, &st)
        .await
        .map_err(code)?;
    let synced = sync_alert(
        &state,
        &claims.team_id,
        &f,
        &json!({
            "state": "dismissed",
            "dismissed_reason": dismissed_reason(&f),
            "dismissed_comment": common::truncate_str(&format!("Dismissed in CoderHelm by {}: {reason}", claims.email), 280),
        }),
    )
    .await;
    add_event(
        &state,
        &claims.team_id,
        &f.id,
        &format!(
            "Dismissed by {}: {reason}{}{}",
            claims.email,
            until
                .as_deref()
                .map(|u| format!(" (until {u})"))
                .unwrap_or_default(),
            if synced {
                ". The Dependabot alert was dismissed too"
            } else {
                ""
            }
        ),
        None,
    )
    .await;
    info!(team = %claims.team_id, finding = %f.id, by = %claims.email, synced, "Security finding dismissed");
    Ok(Json(json!({ "finding": f, "synced": synced })))
}

/// POST /api/security/findings/{id}/reopen
pub async fn reopen(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let (mut f, st) = load_finding(&state, &claims.team_id, &id).await?;
    let was_dismissed = f.status == Status::Dismissed;
    f.status = match &f.pr {
        Some(p) if p.state == "open" => Status::PrOpen,
        _ => Status::Open,
    };
    f.dismissed = None;
    f.updated_at = chrono::Utc::now().to_rfc3339();
    save_finding(&state, &claims.team_id, &f, &st).await?;
    let synced =
        was_dismissed && sync_alert(&state, &claims.team_id, &f, &json!({ "state": "open" })).await;
    add_event(
        &state,
        &claims.team_id,
        &f.id,
        &format!(
            "Reopened by {}{}",
            claims.email,
            if synced {
                ". The Dependabot alert was reopened too"
            } else {
                ""
            }
        ),
        None,
    )
    .await;
    Ok(Json(json!({ "finding": f, "synced": synced })))
}

#[derive(Deserialize, Default)]
pub struct ScanBody {
    #[serde(default)]
    pub repo: Option<String>,
}

/// POST /api/security/scan
pub async fn scan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    body: Option<Json<ScanBody>>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    claims.require_role(3)?;
    let repo = body
        .and_then(|Json(b)| b.repo)
        .map(|r| r.trim().to_string())
        .filter(|r| !r.is_empty());
    if repo.as_deref().is_some_and(|r| !sec::valid_repo(r)) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let msg = WorkerMessage::SecurityEvent(SecurityEventMessage {
        team_id: claims.team_id.clone(),
        installation_id: 0,
        kind: "scan".into(),
        repo: repo.clone().unwrap_or_default(),
        ..Default::default()
    });
    super::github_webhook::send_to_queue(&state, &state.config.ticket_queue_url, &msg).await?;
    info!(team = %claims.team_id, repo = ?repo, by = %claims.email, "Security check queued");
    Ok((StatusCode::ACCEPTED, Json(json!({ "status": "queued" }))))
}

// ── Fix PRs a person asks for ───────────────────────────────────────────

#[derive(Deserialize)]
pub struct FixBody {
    pub finding_ids: Vec<String>,
    /// Raise today's fix-run limit for this request (recorded in the audit log).
    #[serde(default)]
    pub raise_limit: bool,
    /// `one_pr`: one pull request per repository for everything picked,
    /// whatever the policy says; `per_package`: one per package; `policy`
    /// (the default): as the policy's `bundle` says.
    #[serde(default)]
    pub combine: sec::Combine,
}

/// POST /api/security/fix `{finding_ids, raise_limit?}`: fix pull requests
/// for the findings a person picked, one per repository, package and
/// version, started now whatever the dry run says, within the day's limits
/// (an admin may raise today's limit for the request). Admin.
pub async fn fix(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<FixBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    claims.require_role(3).map_err(code)?;
    if body.finding_ids.is_empty() || body.finding_ids.len() > 200 {
        return Err(bad("finding_ids is 1 to 200 finding ids"));
    }
    let cfg = load_config(&state, &claims.team_id)
        .await
        .map_err(code)?
        .unwrap_or_default();
    let findings = load_findings(&state, &claims.team_id).await.map_err(code)?;
    let now = chrono::Utc::now();
    let day = sec::limit_day(now);
    let (mut units, skipped) =
        sec::manual_units_as(&findings, &body.finding_ids, &cfg, now, body.combine);
    let joins: Vec<Option<u64>> = units
        .iter()
        .map(|u| {
            sec::open_bundle_pr(&findings, u, body.combine == sec::Combine::OnePr)
                .map(|(p, _)| p.number)
        })
        .collect();
    let join_of: std::collections::HashMap<String, u64> = units
        .iter()
        .zip(&joins)
        .filter_map(|(u, j)| j.map(|n| (u.ticket.clone(), n)))
        .collect();
    let slots = sec::DynamoSlots {
        dynamo: &state.dynamo,
        settings_table: &state.config.settings_table_name,
        analytics_table: &state.config.analytics_table_name,
        team_id: &claims.team_id,
    };
    let (started, queued) =
        sec::admit_request(&slots, &cfg, &day, &mut units, &findings, body.raise_limit).await;
    if body.raise_limit && !started.is_empty() {
        let _ = super::roles::record(
            &state,
            &claims,
            super::roles::AuditEntry {
                action: "security_fix_limit_raised",
                role_id: "",
                role_name: "",
                detail: format!(
                    "Raised today's fix-run limit ({}) for {} requested fix PR{}",
                    cfg.max_fix_runs_per_day,
                    started.len(),
                    if started.len() == 1 { "" } else { "s" }
                ),
            },
        )
        .await;
    }
    // The started units' runs: the worker opens them (it reads the code).
    let started_ids: Vec<String> = started.iter().flat_map(|r| r.finding_ids.clone()).collect();
    if !started_ids.is_empty() {
        let msg = SecurityEventMessage {
            team_id: claims.team_id.clone(),
            kind: "fix".into(),
            finding_ids: started_ids,
            requested_by: claims.email.clone(),
            combine: body.combine,
            ..Default::default()
        };
        if queue(&state, msg).await.is_err() {
            for _ in &started {
                sec::DailySlots::give_back(&slots, &day).await;
            }
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "The request couldn't be queued. Try again." })),
            ));
        }
    }
    // Every asked finding notes who asked; the queued ones wait with why.
    for (r, waiting) in started
        .iter()
        .map(|r| (r, None))
        .chain(queued.iter().map(|r| (r, r.reason.clone())))
    {
        for id in &r.finding_ids {
            let Some((f, st)) = findings.iter().find(|(f, _)| &f.id == id) else {
                continue;
            };
            let (mut f, mut st) = (f.clone(), st.clone());
            if waiting.is_some() {
                st.requested_by = Some(claims.email.clone());
                st.requested_as = body.combine;
            }
            f.waiting = waiting.clone();
            f.updated_at = now.to_rfc3339();
            let _ = save_finding(&state, &claims.team_id, &f, &st).await;
            add_event(
                &state,
                &claims.team_id,
                &f.id,
                &format!(
                    "Fix PR requested by {}{}",
                    claims.email,
                    waiting
                        .as_deref()
                        .map(|w| format!(". {w}"))
                        .unwrap_or_default()
                ),
                None,
            )
            .await;
        }
    }
    info!(team = %claims.team_id, by = %claims.email, started = started.len(), queued = queued.len(), skipped = skipped.len(), raise = body.raise_limit, "Security fix PRs requested");
    let runs_today = get_item(&state, &claims.team_id, &format!("SECRUNS#{day}"))
        .await
        .ok()
        .flatten()
        .map(|i| num(&i, "runs"))
        .unwrap_or(0);
    Ok(Json(json!({
        "started": started.iter().map(|r| json!({
            "unit": r.unit, "ticket": r.ticket, "repo": r.repo, "package": r.package,
            "finding_ids": r.finding_ids, "run_id": null,
            "runs_url": format!("/?q={}", r.ticket),
            "joins": join_of.get(&r.ticket),
        })).collect::<Vec<_>>(),
        "queued": queued,
        "skipped": skipped,
        "runs_today": runs_today,
        "runs_limit": cfg.max_fix_runs_per_day,
    })))
}

/// POST /api/security/fix/plan `{finding_ids}`: what asking would do, before
/// anyone confirms: the units (one PR each) and the findings it skips, with
/// today's runs against the limit. Any team member.
pub async fn fix_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<FixBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if body.finding_ids.len() > 200 {
        return Err(bad("finding_ids is at most 200 finding ids"));
    }
    let cfg = load_config(&state, &claims.team_id)
        .await
        .map_err(code)?
        .unwrap_or_default();
    let findings = load_findings(&state, &claims.team_id).await.map_err(code)?;
    let now = chrono::Utc::now();
    let (units, skipped) =
        sec::manual_units_as(&findings, &body.finding_ids, &cfg, now, body.combine);
    let runs_today = get_item(
        &state,
        &claims.team_id,
        &format!("SECRUNS#{}", sec::limit_day(now)),
    )
    .await
    .map_err(code)?
    .map(|i| num(&i, "runs"))
    .unwrap_or(0);
    let tokens_today = feature_tokens(
        &state,
        &claims.team_id,
        &format!("DAY#{}", now.format("%Y-%m-%d")),
        common::usage::feature::SECURITY_FIX,
    )
    .await;
    Ok(Json(fix_plan_json(
        &units,
        &skipped,
        &findings,
        &cfg,
        body.combine,
        runs_today,
        tokens_today,
    )))
}

/// Pure: what the confirmation shows. Each unit with its bumps and the
/// open pull request it joins; how many new pull requests that makes; the
/// runs and the token guess against today's limits.
pub fn fix_plan_json(
    units: &[sec::FixUnit],
    skipped: &[sec::Skipped],
    findings: &[(sec::Finding, sec::FindingState)],
    cfg: &SecurityConfig,
    combine: sec::Combine,
    runs_today: u64,
    tokens_today: u64,
) -> Value {
    let plans: Vec<Value> = units
        .iter()
        .map(|u| sec::unit_plan(u, findings, combine == sec::Combine::OnePr))
        .collect();
    let new_prs = plans.iter().filter(|p| p["joins"].is_null()).count();
    let estimated: u64 = units.iter().map(sec::estimate_tokens).sum();
    json!({
        "units": plans,
        "skipped": skipped,
        "combine": combine,
        "bundle": cfg.bundle,
        "pr_count": new_prs,
        "joins": plans.len() - new_prs,
        "runs_today": runs_today,
        "runs_limit": cfg.max_fix_runs_per_day,
        "tokens_today": tokens_today,
        "tokens_limit": cfg.max_fix_tokens_per_day,
        "estimated_tokens": estimated,
    })
}

// ── Settings ────────────────────────────────────────────────────────────

/// GET /api/security/config
pub async fn get_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let cfg = load_config(&state, &claims.team_id)
        .await?
        .unwrap_or_default();
    Ok(Json(
        json!({ "config": cfg, "defaults": SecurityConfig::default() }),
    ))
}

/// PUT /api/security/config
pub async fn put_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(cfg): Json<SecurityConfig>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    claims.require_role(3).map_err(code)?;
    cfg.validate().map_err(|e| bad(&e))?;
    let cfg = cfg.cleaned();
    let text = serde_json::to_string(&cfg).map_err(|_| code(StatusCode::INTERNAL_SERVER_ERROR))?;
    // The same item Setup writes (`common::setup::store::config_item`).
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(common::setup::store::config_item(
            &claims.team_id,
            sec::CONFIG_SK,
            &text,
            &claims.email,
            &chrono::Utc::now().to_rfc3339(),
        )))
        .send()
        .await
        .map_err(|e| {
            error!(error = %e, "security: could not save the settings");
            code(StatusCode::INTERNAL_SERVER_ERROR)
        })?;
    if let Err(e) = common::settings_index::add(
        &state.dynamo,
        &state.config.settings_table_name,
        sec::INDEX_NAME,
        &claims.team_id,
    )
    .await
    {
        // The daily rebuild finds it.
        warn!(error = %e, "security: could not index the team");
    }
    info!(team = %claims.team_id, enabled = cfg.enabled, dry_run = cfg.dry_run, by = %claims.email, "Security settings saved");
    Ok(Json(json!({ "config": cfg })))
}

/// GET /api/security/preview
pub async fn preview(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let cfg = load_config(&state, &claims.team_id)
        .await?
        .unwrap_or_default();
    let findings = load_findings(&state, &claims.team_id).await?;
    Ok(Json(sec::preview(&findings, &cfg, chrono::Utc::now())))
}

// ── Webhooks ────────────────────────────────────────────────────────────

/// Whether the team turned security fixes on.
async fn enabled(state: &AppState, team_id: &str) -> bool {
    matches!(load_config(state, team_id).await, Ok(Some(c)) if c.enabled)
}

async fn queue(state: &AppState, msg: SecurityEventMessage) -> Result<StatusCode, StatusCode> {
    super::github_webhook::send_to_queue(
        state,
        &state.config.ticket_queue_url,
        &WorkerMessage::SecurityEvent(msg),
    )
    .await
}

/// Pure: the event a `dependabot_alert` webhook queues, if any.
pub fn dependabot_event(
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Option<SecurityEventMessage> {
    let action = payload["action"].as_str().unwrap_or("");
    if !matches!(
        action,
        "created"
            | "reopened"
            | "auto_reopened"
            | "reintroduced"
            | "fixed"
            | "dismissed"
            | "auto_dismissed"
            | "withdrawn"
    ) {
        return None;
    }
    let repo = payload["repository"]["full_name"].as_str()?;
    let number = payload["alert"]["number"].as_u64()?;
    sec::valid_repo(repo).then(|| SecurityEventMessage {
        team_id: team_id.to_string(),
        installation_id,
        kind: "dependabot_alert".into(),
        repo: repo.to_string(),
        action: action.to_string(),
        alert_number: number,
        ghsa_id: payload["alert"]["security_advisory"]["ghsa_id"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        ..Default::default()
    })
}

/// Pure: the event a `security_advisory` webhook queues. `team_id` empty:
/// every team with security fixes on.
pub fn advisory_event(
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Option<SecurityEventMessage> {
    let action = payload["action"].as_str().unwrap_or("");
    if !matches!(action, "published" | "updated" | "withdrawn") {
        return None;
    }
    let ghsa = payload["security_advisory"]["ghsa_id"].as_str()?;
    (ghsa.starts_with("GHSA-") && ghsa.len() < 40).then(|| SecurityEventMessage {
        team_id: team_id.to_string(),
        installation_id,
        kind: "security_advisory".into(),
        action: action.to_string(),
        ghsa_id: ghsa.to_string(),
        ..Default::default()
    })
}

/// Pure: whether a push changed a dependency file.
pub fn push_touches_dependencies(payload: &Value) -> bool {
    payload["commits"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|c| {
            ["added", "modified", "removed"].iter().any(|k| {
                c[*k]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|p| p.as_str())
                    .any(sec::is_dependency_file)
            })
        })
}

/// The `dependabot_alert` webhook.
pub(crate) async fn on_dependabot_alert(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let Some(msg) = dependabot_event(payload, installation_id, team_id) else {
        return Ok(StatusCode::OK);
    };
    if !enabled(state, team_id).await {
        return Ok(StatusCode::OK);
    }
    info!(team_id, repo = %msg.repo, action = %msg.action, alert = msg.alert_number, "Dependabot alert queued for security fixes");
    queue(state, msg).await
}

/// The `security_advisory` webhook (`team_id` empty when GitHub sent it
/// without an installation).
pub(crate) async fn on_security_advisory(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let Some(msg) = advisory_event(payload, installation_id, team_id) else {
        return Ok(StatusCode::OK);
    };
    if !team_id.is_empty() && !enabled(state, team_id).await {
        return Ok(StatusCode::OK);
    }
    info!(team_id, ghsa = %msg.ghsa_id, action = %msg.action, "GitHub advisory queued for security fixes");
    queue(state, msg).await
}

/// A push to a default branch that changed a dependency file.
pub(crate) async fn on_default_branch_push(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
    repo: &str,
) {
    if !push_touches_dependencies(payload) || !enabled(state, team_id).await {
        return;
    }
    let msg = SecurityEventMessage {
        team_id: team_id.to_string(),
        installation_id,
        kind: "push".into(),
        repo: repo.to_string(),
        ..Default::default()
    };
    if let Err(e) = queue(state, msg).await {
        warn!(team_id, repo, code = %e, "security: could not queue the inventory refresh");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(id: &str, tier: Tier, status: Status, due: &str, repo: &str) -> Finding {
        Finding {
            id: id.into(),
            tier: Some(tier),
            status,
            due_at: due.into(),
            repo: repo.into(),
            ..Default::default()
        }
    }

    #[test]
    fn findings_are_filtered_sorted_and_paged() {
        let all = vec![
            f("a", Tier::P2, Status::Open, "2026-12-01", "acme/web"),
            f("b", Tier::P0, Status::PrOpen, "2026-11-12", "acme/web"),
            f("c", Tier::P0, Status::Open, "2026-11-11", "acme/api"),
            f("d", Tier::P1, Status::Fixed, "2026-11-20", "acme/web"),
            f("e", Tier::P3, Status::Dismissed, "2027-01-01", "acme/web"),
        ];
        let q = ListQuery::default();
        let (page, next) = list_page(all.clone(), &q).unwrap();
        assert_eq!(
            page.iter().map(|f| f.id.as_str()).collect::<Vec<_>>(),
            vec!["c", "b", "a"]
        );
        assert_eq!(next, None);
        let q = ListQuery {
            status: Some("fixed".into()),
            ..Default::default()
        };
        assert_eq!(list_page(all.clone(), &q).unwrap().0.len(), 1);
        let q = ListQuery {
            status: Some("all".into()),
            tier: Some("p0".into()),
            repo: Some("acme/web".into()),
            ..Default::default()
        };
        assert_eq!(list_page(all.clone(), &q).unwrap().0[0].id, "b");
        for bad in [
            ListQuery {
                status: Some("weird".into()),
                ..Default::default()
            },
            ListQuery {
                tier: Some("P9".into()),
                ..Default::default()
            },
            ListQuery {
                repo: Some("../x".into()),
                ..Default::default()
            },
            ListQuery {
                cursor: Some("x".into()),
                ..Default::default()
            },
        ] {
            assert_eq!(
                list_page(all.clone(), &bad).unwrap_err(),
                StatusCode::BAD_REQUEST
            );
        }
        let many: Vec<Finding> = (0..120)
            .map(|i| {
                f(
                    &format!("{i:03}"),
                    Tier::P2,
                    Status::Open,
                    "2026-12-01",
                    "acme/web",
                )
            })
            .collect();
        let (p1, n1) = list_page(many.clone(), &ListQuery::default()).unwrap();
        assert_eq!((p1.len(), n1.as_deref()), (50, Some("50")));
        let (p3, n3) = list_page(
            many,
            &ListQuery {
                cursor: Some("100".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!((p3.len(), n3), (20, None));
    }

    #[test]
    fn a_dismissal_needs_a_reason_and_a_future_date() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 11, 10).unwrap();
        let b = |r: &str, u: Option<&str>| DismissBody {
            reason: r.into(),
            until: u.map(String::from),
        };
        assert!(check_dismiss(&b(" ", None), today).is_err());
        assert!(check_dismiss(&b(&"x".repeat(501), None), today).is_err());
        assert!(check_dismiss(&b("build only", Some("soon")), today).is_err());
        assert!(check_dismiss(&b("build only", Some("2026-11-09")), today).is_err());
        assert_eq!(
            check_dismiss(&b(" build only ", Some("2027-01-31")), today).unwrap(),
            ("build only".to_string(), Some("2027-01-31".to_string()))
        );
        assert_eq!(check_dismiss(&b("ok", Some("")), today).unwrap().1, None);
        let mut fi = Finding::default();
        assert_eq!(dismissed_reason(&fi), "tolerable_risk");
        fi.verdict = sec::Verdict::NotLoaded;
        assert_eq!(dismissed_reason(&fi), "not_used");
    }

    #[test]
    fn webhooks_queue_only_what_matters() {
        let p = json!({"action": "created", "repository": {"full_name": "acme/web"}, "alert": {"number": 88, "security_advisory": {"ghsa_id": "GHSA-q9fx-2w5c-8m4r"}}});
        let m = dependabot_event(&p, 7, "TEAM#a").unwrap();
        assert_eq!(
            (m.kind.as_str(), m.repo.as_str(), m.alert_number),
            ("dependabot_alert", "acme/web", 88)
        );
        for action in [
            "fixed",
            "dismissed",
            "auto_dismissed",
            "withdrawn",
            "reopened",
        ] {
            let mut q = p.clone();
            q["action"] = json!(action);
            assert!(dependabot_event(&q, 7, "TEAM#a").is_some(), "{action}");
        }
        let mut q = p.clone();
        q["action"] = json!("assignees_changed");
        assert!(dependabot_event(&q, 7, "TEAM#a").is_none());
        let adv =
            json!({"action": "withdrawn", "security_advisory": {"ghsa_id": "GHSA-q9fx-2w5c-8m4r"}});
        let m = advisory_event(&adv, 0, "").unwrap();
        assert!(m.team_id.is_empty() && m.action == "withdrawn");
        assert!(advisory_event(
            &json!({"action": "published", "security_advisory": {"ghsa_id": "nope"}}),
            0,
            ""
        )
        .is_none());
        let push =
            json!({"commits": [{"added": [], "modified": ["web/pnpm-lock.yaml"], "removed": []}]});
        assert!(push_touches_dependencies(&push));
        assert!(!push_touches_dependencies(
            &json!({"commits": [{"modified": ["src/a.ts"]}]})
        ));
    }

    /// What the confirmation shows for picked rows: one PR per repo for
    /// "one PR" whatever the policy says, the open PR it joins, and the
    /// runs and tokens against today's limits.
    #[test]
    fn the_fix_plan_counts_prs_joins_and_the_limits() {
        let mk = |id: &str, repo: &str, pkg: &str| {
            let mut f = Finding {
                id: id.into(),
                repo: repo.into(),
                ecosystem: "npm".into(),
                package: pkg.into(),
                version: "1.0.0".into(),
                fixed_version: Some("2.0.0".into()),
                tier: Some(Tier::P1),
                status: Status::Open,
                ..Default::default()
            };
            f.advisory.primary = format!("GHSA-{id}");
            f.advisory.ids = vec![f.advisory.primary.clone()];
            (f, sec::FindingState::default())
        };
        let mut findings = vec![
            mk("a", "acme/web", "tar"),
            mk("b", "acme/web", "ws"),
            mk("c", "acme/api", "qs"),
            mk("open", "acme/api", "undici"),
        ];
        findings[3].0.status = Status::PrOpen;
        findings[3].0.pr = Some(sec::PrRef {
            number: 812,
            url: "https://github.com/acme/api/pull/812".into(),
            state: "open".into(),
            ..Default::default()
        });
        findings[3].1.bundled = true;
        findings[3].1.ticket = Some("SEC-OPEN".into());
        let cfg = SecurityConfig {
            bundle: sec::Bundle::PerPackage,
            ..Default::default()
        };
        let ids: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let now = chrono::Utc::now();
        let (units, skipped) =
            sec::manual_units_as(&findings, &ids, &cfg, now, sec::Combine::OnePr);
        let plan = fix_plan_json(
            &units,
            &skipped,
            &findings,
            &cfg,
            sec::Combine::OnePr,
            1,
            310_000,
        );
        assert_eq!(
            plan["units"].as_array().unwrap().len(),
            2,
            "one per repo, whatever the policy"
        );
        assert_eq!(plan["pr_count"], 1, "the api fix joins its open PR");
        assert_eq!(plan["joins"], 1);
        let api = plan["units"]
            .as_array()
            .unwrap()
            .iter()
            .find(|u| u["repo"] == "acme/api")
            .unwrap();
        assert_eq!(api["joins"]["number"], 812);
        let web = plan["units"]
            .as_array()
            .unwrap()
            .iter()
            .find(|u| u["repo"] == "acme/web")
            .unwrap();
        assert_eq!(web["bumps"].as_array().unwrap().len(), 2);
        assert_eq!(web["majors"], 2);
        assert_eq!(
            (plan["runs_today"].as_u64(), plan["runs_limit"].as_u64()),
            (Some(1), Some(3))
        );
        assert_eq!(plan["tokens_today"], 310_000);
        assert_eq!(plan["tokens_limit"], 2_000_000);
        assert!(plan["estimated_tokens"].as_u64().unwrap() > 0);
        // Per package: three units, nothing joins.
        let (units, _) = sec::manual_units_as(&findings, &ids, &cfg, now, sec::Combine::PerPackage);
        let plan = fix_plan_json(&units, &[], &findings, &cfg, sec::Combine::PerPackage, 0, 0);
        assert_eq!(
            (plan["pr_count"].as_u64(), plan["joins"].as_u64()),
            (Some(3), Some(0))
        );
    }

    #[test]
    fn a_saved_config_is_validated() {
        let ok: SecurityConfig =
            serde_json::from_value(json!({"enabled": true, "labels": ["security"]})).unwrap();
        assert!(
            ok.validate().is_ok() && !ok.dry_run,
            "turned on: opens PRs within the limits"
        );
        let bad: SecurityConfig =
            serde_json::from_value(json!({"due_days": {"P0": 0, "P1": 7, "P2": 30, "P3": 90}}))
                .unwrap();
        assert!(bad.validate().is_err());
    }

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| {
                let a = if v.chars().all(|c| c.is_ascii_digit()) && !v.is_empty() {
                    AttributeValue::N(v.to_string())
                } else {
                    attr_s(v)
                };
                (k.to_string(), a)
            })
            .collect()
    }

    /// The Sources panel: one row per source with its state in plain words,
    /// a last sync and the findings that came from it.
    #[test]
    fn sources_say_their_state_and_what_came_from_them() {
        let mut osv = f(
            "o",
            Tier::P2,
            Status::Open,
            "2026-12-01T00:00:00Z",
            "acme/web",
        );
        osv.advisory.source = "osv".into();
        let mut kev = f(
            "k",
            Tier::P0,
            Status::Open,
            "2026-11-14T00:00:00Z",
            "acme/web",
        );
        kev.advisory.source = "github_advisory".into();
        kev.advisory.kev = true;
        let fs = vec![
            (osv, FindingState::default()),
            (kev, FindingState::default()),
        ];
        let st = item(&[
            ("dependabot", "not_accepted"),
            ("dependabot_reason", "The GitHub App asks for Dependabot alerts: read, but the organization hasn't accepted it yet."),
            ("osv_synced_at", "2026-11-11T02:27:15Z"),
        ]);
        let rows = source_rows(
            &st,
            &item(&[("synced_at", "2026-11-11T02:22:00Z"), ("count", "1484")]),
            &fs,
        );
        let by = |id: &str| {
            rows.as_array()
                .unwrap()
                .iter()
                .find(|r| r["id"] == id)
                .unwrap()
                .clone()
        };
        assert_eq!(by("dependabot")["state"], "needs_permission");
        assert!(by("dependabot")["reason"]
            .as_str()
            .unwrap()
            .contains("hasn't accepted"));
        assert_eq!(by("osv")["state"], "ok");
        assert_eq!(by("osv")["count"], 1);
        assert_eq!(by("github_advisory")["count"], 1);
        assert_eq!(by("kev")["count"], 1);
        assert_eq!(by("kev")["listed"], 1484);
        assert_eq!(by("epss")["state"], "never");
        let err = source_rows(
            &item(&[("osv_error", "OSV answered 503")]),
            &Item::new(),
            &fs,
        );
        assert_eq!(err[1]["state"], "error");
        assert_eq!(
            err[1]["reason"],
            "OSV answered 503; tried again on the next check."
        );
    }

    #[test]
    fn the_inventory_lists_each_repo_or_why_it_couldnt_be_read() {
        let deps = vec![
            item(&[("repo", "acme/api"), ("count", "1304"), ("ecosystems", r#"{"npm":1301,"docker":1}"#), ("read_at", "2026-11-11T02:26:24Z"), ("inventory_state", "ok")]),
            item(&[("repo", "acme/web"), ("count", "1"), ("ecosystems", r#"{"docker":1}"#), ("inventory_state", "graph_off"), ("inventory_reason", "GitHub's dependency graph is off for this repo; turn it on in the repo's Settings, Code security.")]),
        ];
        let cfg = SecurityConfig {
            repos: sec::Repos::List(vec![
                "acme/api".into(),
                "acme/web".into(),
                "acme/docs".into(),
            ]),
            ..Default::default()
        };
        let rows = repo_rows(&deps, &cfg, &Item::new());
        let r = rows.as_array().unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0]["next_read"], NEXT_READ);
        assert_eq!(r[1]["next_read"], "", "not read yet: no next read to name");
        assert_eq!(r[0]["repo"], "acme/api");
        assert_eq!(r[0]["packages"], 1304);
        assert_eq!(r[0]["ecosystems"]["npm"], 1301);
        assert_eq!(r[1]["repo"], "acme/docs");
        assert_eq!(r[1]["state"], "not_read");
        assert_eq!(r[2]["state"], "graph_off");
        assert!(r[2]["reason"]
            .as_str()
            .unwrap()
            .contains("Settings, Code security"));
    }

    #[test]
    fn a_repos_dependabot_state_shows_only_when_it_differs_from_the_teams() {
        let why = "GitHub turned down the request for Dependabot's alerts (400: x); it is tried again on the next check.";
        let team = item(&[("dependabot", "error"), ("dependabot_reason", why)]);
        let deps = vec![
            item(&[
                ("repo", "acme/api"),
                ("read_at", "2026-11-11T05:38:55Z"),
                ("dependabot", "error"),
                ("dependabot_reason", why),
            ]),
            item(&[
                ("repo", "acme/web"),
                ("read_at", "2026-11-11T05:38:55Z"),
                ("dependabot", "off"),
                (
                    "dependabot_reason",
                    "Dependabot alerts are turned off for this repo.",
                ),
            ]),
        ];
        let rows = repo_rows(&deps, &SecurityConfig::default(), &team);
        let r = rows.as_array().unwrap();
        assert_eq!(
            r[0]["dependabot"],
            Value::Null,
            "same as the team's row: not repeated"
        );
        assert_eq!(r[0]["dependabot_reason"], "");
        assert_eq!(r[1]["dependabot"], "off");
        assert!(r[1]["dependabot_reason"]
            .as_str()
            .unwrap()
            .contains("turned off"));
    }

    #[test]
    fn the_advisory_feed_counts_the_poll_until_a_webhook_arrives() {
        let row = |st: &Item| {
            source_rows(st, &Item::new(), &[])
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["id"] == "github_advisory")
                .unwrap()
                .clone()
        };
        let none = row(&Item::new());
        assert_eq!(none["state"], "never");
        assert!(none["reason"]
            .as_str()
            .unwrap()
            .starts_with("Waiting for GitHub's first webhook"));
        let polled = row(&item(&[("advisory_polled_at", "2026-11-11T06:00:00Z")]));
        assert_eq!(polled["state"], "ok");
        assert_eq!(polled["synced_at"], "2026-11-11T06:00:00Z");
        assert_eq!(
            polled["reason"],
            "Waiting for GitHub's first webhook; checked by polling at Nov 11, 06:00 UTC."
        );
        // Last read is whichever ran last; a webhook clears the note.
        let both = row(&item(&[
            ("advisory_event_at", "2026-11-11T05:14:15Z"),
            ("advisory_polled_at", "2026-11-11T06:00:00Z"),
        ]));
        assert_eq!(both["synced_at"], "2026-11-11T06:00:00Z");
        assert_eq!(both["reason"], "");
        let hook_last = row(&item(&[
            ("advisory_event_at", "2026-11-11T07:00:00Z"),
            ("advisory_polled_at", "2026-11-11T06:00:00Z"),
        ]));
        assert_eq!(hook_last["synced_at"], "2026-11-11T07:00:00Z");
    }

    #[test]
    fn groups_page_filters_searches_and_sorts() {
        let mk = |id: &str, pkg: &str, ver: &str, tier: Tier, due: &str| {
            let mut x = f(id, tier, Status::Open, due, "acme/web");
            x.package = pkg.into();
            x.version = ver.into();
            x.advisory.ids = vec![format!("GHSA-{id}")];
            x.advisory.title = format!("{pkg} issue");
            (x, FindingState::default())
        };
        let fs = vec![
            mk("a", "tar", "6.2.1", Tier::P1, "2026-11-17T00:00:00Z"),
            mk("b", "tar", "6.2.1", Tier::P2, "2026-12-10T00:00:00Z"),
            mk("c", "axios", "1.8.3", Tier::P2, "2026-11-20T00:00:00Z"),
        ];
        let (page, total, next) = group_page(&fs, &GroupQuery::default()).unwrap();
        assert_eq!((total, next), (2, None));
        assert_eq!(page[0].package, "tar");
        assert_eq!(page[0].advisories.len(), 2);
        let q = GroupQuery {
            q: Some("GHSA-c".into()),
            ..Default::default()
        };
        assert_eq!(group_page(&fs, &q).unwrap().0[0].package, "axios");
        let q = GroupQuery {
            sort: Some("package".into()),
            ..Default::default()
        };
        assert_eq!(group_page(&fs, &q).unwrap().0[0].package, "axios");
        let q = GroupQuery {
            sort: Some("sideways".into()),
            ..Default::default()
        };
        assert!(group_page(&fs, &q).is_err());
    }
}
