//! The platform admin's view of self-hosted installs that use CoderHelm's
//! hosted parts (`/api/admin/hybrid/summary`, `/api/admin/hybrid/:id`),
//! hosted deployment only, platform admins only.
//!
//! It reads only what hybrid already keeps: the install records, their
//! delivery records (ids, types, status), the mail relay's counters and the
//! audit log. No payloads, no email contents; recipient addresses only as
//! counts.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Duration, Utc};
use common::deliveries::{self, Delivery};
use common::hybrid::relay as hybrid_relay;
use common::mail::relay;
use serde::Serialize;
use serde_json::{json, Value};

use super::hybrid::{fail, github_links, load_install, master, not_here, operator, Install};
use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn not_admin(c: StatusCode) -> (StatusCode, Json<Value>) {
    fail(c, "forbidden", "Platform admins only.")
}

/// One day's mail relay counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct DayCounts {
    pub sent: u64,
    pub bounced: u64,
    pub complained: u64,
}

/// What the admin page shows about one install.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct InstallStats {
    pub install_id: String,
    pub name: Option<String>,
    pub hostname: Option<String>,
    pub cloudfront: String,
    pub version: String,
    pub registered_at: String,
    pub last_seen: String,
    /// `active`, `paused` (mail relay paused) or `revoked`.
    pub status: String,
    pub verified: bool,
    pub name_live: bool,
    pub sharing: bool,
    pub integrations: Vec<String>,
    pub github_links: usize,
    pub relay_24h: u64,
    pub relay_7d: u64,
    pub relay_failed_7d: u64,
    pub broker_7d: u64,
    pub email_opted_in: bool,
    pub email_paused: bool,
    pub emails_24h: u64,
    pub emails_7d: u64,
    pub bounced_7d: u64,
    pub complained_7d: u64,
    pub listed_recipients: u64,
    pub parts: Vec<&'static str>,
}

/// The day (`YYYY-MM-DD`) a timestamp falls on, if it parses.
fn day_of(ts: &str) -> Option<String> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|t| t.with_timezone(&Utc).format("%Y-%m-%d").to_string())
}

fn within(ts: &str, now: DateTime<Utc>, d: Duration) -> bool {
    DateTime::parse_from_rfc3339(ts)
        .map(|t| now - t.with_timezone(&Utc) <= d)
        .unwrap_or(false)
}

/// Builds one install's numbers from what was read. Pure.
#[allow(clippy::too_many_arguments)]
pub fn install_stats(
    i: &Install,
    deliveries: &[Delivery],
    days: &BTreeMap<String, DayCounts>,
    links: usize,
    paused: bool,
    opted_in: bool,
    listed: u64,
    now: DateTime<Utc>,
) -> InstallStats {
    let week = Duration::days(7);
    let day = Duration::hours(24);
    let relay: Vec<&Delivery> = deliveries.iter().filter(|d| d.source == "relay").collect();
    let relay_7d = relay
        .iter()
        .filter(|d| within(&d.created_at, now, week))
        .count() as u64;
    let relay_24h = relay
        .iter()
        .filter(|d| within(&d.created_at, now, day))
        .count() as u64;
    let relay_failed_7d = relay
        .iter()
        .filter(|d| within(&d.created_at, now, week) && !d.status.is_ok())
        .count() as u64;
    let broker_7d = deliveries
        .iter()
        .filter(|d| d.source == "broker" && within(&d.created_at, now, week))
        .count() as u64;
    let emails_24h = deliveries
        .iter()
        .filter(|d| {
            d.source == "email_relay" && d.status.is_ok() && within(&d.created_at, now, day)
        })
        .count() as u64;
    let cutoff = (now - week).format("%Y-%m-%d").to_string();
    let (mut emails_7d, mut bounced_7d, mut complained_7d) = (0, 0, 0);
    for (d, c) in days {
        if d.as_str() > cutoff.as_str() {
            emails_7d += c.sent;
            bounced_7d += c.bounced;
            complained_7d += c.complained;
        }
    }
    let last_seen = deliveries
        .iter()
        .map(|d| d.created_at.as_str())
        .max()
        .unwrap_or(i.verified_at.as_str())
        .to_string();
    let name = (!i.name.is_empty()).then(|| i.name.clone());
    let mut parts = Vec::new();
    if name.is_some() {
        parts.push("address");
    }
    if links > 0 {
        parts.push("github");
    }
    for p in ["jira", "notion", "confluence", "google_drive"] {
        if i.integrations.iter().any(|x| x == p) {
            parts.push(p);
        }
    }
    if broker_7d > 0 {
        parts.push("broker");
    }
    if opted_in {
        parts.push("email");
    }
    if i.sharing {
        parts.push("sharing");
    }
    let status = if !i.active() {
        "revoked"
    } else if paused {
        "paused"
    } else {
        "active"
    };
    InstallStats {
        install_id: i.id.clone(),
        hostname: name.as_ref().map(|n| common::hybrid::names::hostname(n)),
        name,
        cloudfront: i.cloudfront.clone(),
        version: i.version.clone(),
        registered_at: i.created_at.clone(),
        last_seen,
        status: status.into(),
        verified: i.verified(),
        name_live: i.verified() && !i.name.is_empty(),
        sharing: i.sharing,
        integrations: i.integrations.clone(),
        github_links: links,
        relay_24h,
        relay_7d,
        relay_failed_7d,
        broker_7d,
        email_opted_in: opted_in,
        email_paused: paused,
        emails_24h,
        emails_7d,
        bounced_7d,
        complained_7d,
        listed_recipients: listed,
        parts,
    }
}

fn rate(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        ((part as f64 / whole as f64) * 10_000.0).round() / 10_000.0
    }
}

/// The page's summary over every install. Pure.
pub fn summarize(stats: &[InstallStats], trend: &[Value], now: DateTime<Utc>) -> Value {
    let active_7d = stats
        .iter()
        .filter(|s| s.status != "revoked" && within(&s.last_seen, now, Duration::days(7)))
        .count();
    let count = |f: &dyn Fn(&InstallStats) -> bool| stats.iter().filter(|s| f(s)).count();
    let sum = |f: &dyn Fn(&InstallStats) -> u64| stats.iter().map(f).sum::<u64>();
    let relay_7d = sum(&|s| s.relay_7d);
    let emails_7d = sum(&|s| s.emails_7d);
    let integrations: serde_json::Map<String, Value> =
        ["jira", "notion", "confluence", "google_drive"]
            .iter()
            .map(|p| {
                (
                    p.to_string(),
                    json!(count(&|s| s.integrations.iter().any(|x| x == p))),
                )
            })
            .collect();
    json!({
        "installs": {
            "total": stats.len(),
            "active_7d": active_7d,
            "revoked": count(&|s| s.status == "revoked"),
        },
        "address": {
            "claimed": count(&|s| s.name.is_some()),
            "live": count(&|s| s.name_live),
        },
        "github": {
            "installs": count(&|s| s.github_links > 0),
            "linked_installations": stats.iter().map(|s| s.github_links).sum::<usize>(),
            "relayed_24h": sum(&|s| s.relay_24h),
            "relayed_7d": relay_7d,
            "failure_rate_7d": rate(sum(&|s| s.relay_failed_7d), relay_7d),
        },
        "integrations": integrations,
        "broker": {
            "installs": count(&|s| s.broker_7d > 0),
            "tokens_7d": sum(&|s| s.broker_7d),
            "tokens_per_day": rate(sum(&|s| s.broker_7d), 7),
        },
        "email": {
            "opted_in": count(&|s| s.email_opted_in),
            "sent_24h": sum(&|s| s.emails_24h),
            "sent_7d": emails_7d,
            "bounce_rate_7d": rate(sum(&|s| s.bounced_7d), emails_7d),
            "complaint_rate_7d": rate(sum(&|s| s.complained_7d), emails_7d),
            "paused": count(&|s| s.email_paused),
            "recipients": sum(&|s| s.listed_recipients),
        },
        "sharing": count(&|s| s.sharing),
        "trend": trend,
    })
}

/// Relay events and emails per day over the last 7 days. Pure.
pub fn trend(
    per_install: &[(Vec<Delivery>, BTreeMap<String, DayCounts>)],
    now: DateTime<Utc>,
) -> Vec<Value> {
    let mut days: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for back in (0..7).rev() {
        days.insert(
            (now - Duration::days(back)).format("%Y-%m-%d").to_string(),
            (0, 0),
        );
    }
    for (ds, counts) in per_install {
        for d in ds.iter().filter(|d| d.source == "relay") {
            if let Some(e) = day_of(&d.created_at).and_then(|k| days.get_mut(&k)) {
                e.0 += 1;
            }
        }
        for (k, c) in counts {
            if let Some(e) = days.get_mut(k) {
                e.1 += c.sent;
            }
        }
    }
    days.into_iter()
        .map(|(day, (events, emails))| json!({ "day": day, "events": events, "emails": emails }))
        .collect()
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn num(i: &HashMap<String, AttributeValue>, k: &str) -> u64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

async fn query_prefix(
    state: &AppState,
    pk: &str,
    prefix: &str,
    count_only: bool,
) -> (Vec<HashMap<String, AttributeValue>>, u64) {
    let mut q = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :p AND begins_with(sk, :x)")
        .expression_attribute_values(":p", s(pk))
        .expression_attribute_values(":x", s(prefix));
    if count_only {
        q = q.select(aws_sdk_dynamodb::types::Select::Count);
    }
    match q.send().await {
        Ok(r) => (r.items().to_vec(), r.count() as u64),
        Err(_) => (vec![], 0),
    }
}

async fn read_install(
    state: &AppState,
    i: &Install,
    now: DateTime<Utc>,
) -> (InstallStats, Vec<Delivery>, BTreeMap<String, DayCounts>) {
    let pk = hybrid_relay::install_pk(&i.id);
    let table = &state.config.settings_table_name;
    let ds = deliveries::list(
        &state.dynamo,
        table,
        &pk,
        &deliveries::Filter::default(),
        1000,
    )
    .await
    .unwrap_or_default();
    let (day_items, _) = query_prefix(state, &pk, "EMAILDAY#", false).await;
    let days: BTreeMap<String, DayCounts> = day_items
        .iter()
        .filter_map(|it| {
            let sk = it.get("sk")?.as_s().ok()?;
            Some((
                sk.trim_start_matches("EMAILDAY#").to_string(),
                DayCounts {
                    sent: num(it, "sent"),
                    bounced: num(it, "bounced"),
                    complained: num(it, "complained"),
                },
            ))
        })
        .collect();
    let (_, listed) = query_prefix(state, &pk, "EMAILTO#", true).await;
    let (pause, _) = query_prefix(state, &pk, relay::PAUSE_SK, false).await;
    let (list_meta, _) = query_prefix(state, &pk, "EMAILLIST", false).await;
    let links = github_links(state, &i.id).await.len();
    let st = install_stats(
        i,
        &ds,
        &days,
        links,
        !pause.is_empty(),
        !list_meta.is_empty() || listed > 0,
        listed,
        now,
    );
    (st, ds, days)
}

async fn all_installs(state: &AppState) -> Vec<Install> {
    let (rows, _) = query_prefix(state, "HYBRIDINSTALLS", "", false).await;
    let mut out = Vec::new();
    for r in rows {
        let Some(id) = r.get("sk").and_then(|v| v.as_s().ok()) else {
            continue;
        };
        if let Ok(Some(i)) = load_install(state, id).await {
            out.push(i);
        }
    }
    out
}

/// GET /api/admin/hybrid/summary: every install's numbers and the totals.
pub async fn summary(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(not_admin)?;
    operator(&claims)?;
    master(&state).ok_or_else(not_here)?;
    let now = Utc::now();
    let installs = all_installs(&state).await;
    let mut stats = Vec::new();
    let mut raw = Vec::new();
    for chunk in installs.chunks(16) {
        let read =
            futures::future::join_all(chunk.iter().map(|i| read_install(&state, i, now))).await;
        for (st, ds, days) in read {
            stats.push(st);
            raw.push((ds, days));
        }
    }
    let t = trend(&raw, now);
    let mut out = summarize(&stats, &t, now);
    out["rows"] = json!(stats);
    out["generated_at"] = json!(now.to_rfc3339());
    Ok(Json(out))
}

/// GET /api/admin/hybrid/:install_id: one install's numbers and its audit log.
pub async fn detail(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(not_admin)?;
    operator(&claims)?;
    master(&state).ok_or_else(not_here)?;
    let install = load_install(&state, &id)
        .await?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "not_found", "No such install."))?;
    let now = Utc::now();
    let (st, ds, days) = read_install(&state, &install, now).await;
    let audit = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :p")
        .filter_expression("install_id = :i")
        .expression_attribute_values(":p", s("HYBRIDAUDIT"))
        .expression_attribute_values(":i", s(&install.id))
        .scan_index_forward(false)
        .limit(1000)
        .send()
        .await
        .map(|r| r.items().to_vec())
        .unwrap_or_default();
    let audit: Vec<Value> = audit
        .iter()
        .map(|i| {
            let g = |k: &str| {
                i.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            json!({ "action": g("action"), "at": g("at"), "detail": g("detail") })
        })
        .collect();
    Ok(Json(json!({
        "install": st,
        "trend": trend(&[(ds, days)], now),
        "audit": audit,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::deliveries::{Direction, Status};

    fn inst(id: &str, name: &str, status: &str) -> Install {
        Install {
            id: id.into(),
            status: status.into(),
            cloudfront: "d1example.cloudfront.net".into(),
            verified_at: "2026-10-20T00:00:00Z".into(),
            name: name.into(),
            created_at: "2026-10-01T00:00:00Z".into(),
            version: "v1.2.3".into(),
            integrations: vec!["notion".into()],
            ..Default::default()
        }
    }

    fn del(source: &str, at: &str, ok: bool) -> Delivery {
        let mut d = Delivery::new(Direction::Outbound, source, "x", "y");
        d.created_at = at.into();
        d.finish(if ok { Status::Handled } else { Status::Failed }, "", "");
        d
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-26T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn one_installs_numbers() {
        let ds = vec![
            del("relay", "2026-10-26T10:00:00Z", true),
            del("relay", "2026-10-24T10:00:00Z", false),
            del("relay", "2026-10-10T10:00:00Z", true),
            del("broker", "2026-10-25T10:00:00Z", true),
            del("email_relay", "2026-10-26T09:00:00Z", true),
        ];
        let mut days = BTreeMap::new();
        days.insert(
            "2026-10-25".into(),
            DayCounts {
                sent: 40,
                bounced: 2,
                complained: 0,
            },
        );
        days.insert(
            "2026-10-01".into(),
            DayCounts {
                sent: 99,
                bounced: 9,
                complained: 9,
            },
        );
        let st = install_stats(
            &inst("ins_a", "example", "active"),
            &ds,
            &days,
            2,
            false,
            true,
            12,
            now(),
        );
        assert_eq!((st.relay_24h, st.relay_7d, st.relay_failed_7d), (1, 2, 1));
        assert_eq!(st.broker_7d, 1);
        assert_eq!((st.emails_24h, st.emails_7d, st.bounced_7d), (1, 40, 2));
        assert_eq!(st.last_seen, "2026-10-26T10:00:00Z");
        assert_eq!(st.hostname.as_deref(), Some("example.coderhelm.com"));
        assert_eq!(st.status, "active");
        assert!(
            st.parts.contains(&"address")
                && st.parts.contains(&"github")
                && st.parts.contains(&"email")
                && st.parts.contains(&"notion")
        );
        let paused = install_stats(
            &inst("ins_b", "", "active"),
            &[],
            &BTreeMap::new(),
            0,
            true,
            true,
            0,
            now(),
        );
        assert_eq!(paused.status, "paused");
        assert_eq!(paused.last_seen, "2026-10-20T00:00:00Z");
        let revoked = install_stats(
            &inst("ins_c", "", "revoked"),
            &[],
            &BTreeMap::new(),
            0,
            false,
            false,
            0,
            now(),
        );
        assert_eq!(revoked.status, "revoked");
    }

    #[test]
    fn the_summary_counts_installs_and_rates_with_no_addresses() {
        let mut days = BTreeMap::new();
        days.insert(
            "2026-10-25".into(),
            DayCounts {
                sent: 100,
                bounced: 3,
                complained: 1,
            },
        );
        let a = install_stats(
            &inst("ins_a", "example", "active"),
            &[del("relay", "2026-10-26T10:00:00Z", true)],
            &days,
            1,
            false,
            true,
            7,
            now(),
        );
        let b = install_stats(
            &inst("ins_b", "", "revoked"),
            &[],
            &BTreeMap::new(),
            0,
            false,
            false,
            0,
            now(),
        );
        let v = summarize(&[a, b], &[], now());
        assert_eq!(v["installs"]["total"], 2);
        assert_eq!(v["installs"]["active_7d"], 1);
        assert_eq!(v["installs"]["revoked"], 1);
        assert_eq!(v["email"]["opted_in"], 1);
        assert_eq!(v["email"]["sent_7d"], 100);
        assert_eq!(v["email"]["bounce_rate_7d"], 0.03);
        assert_eq!(v["email"]["recipients"], 7);
        assert_eq!(v["integrations"]["notion"], 2);
        assert!(!v.to_string().contains('@'));
    }

    #[test]
    fn the_trend_has_seven_days() {
        let mut days = BTreeMap::new();
        days.insert(
            "2026-10-26".into(),
            DayCounts {
                sent: 5,
                ..Default::default()
            },
        );
        let t = trend(
            &[(vec![del("relay", "2026-10-26T01:00:00Z", true)], days)],
            now(),
        );
        assert_eq!(t.len(), 7);
        assert_eq!(t[6], json!({"day": "2026-10-26", "events": 1, "emails": 5}));
        assert_eq!(t[0]["day"], "2026-10-20");
    }
}
