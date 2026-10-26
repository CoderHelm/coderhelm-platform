//! The Deliveries page: every event that came in and every message that went
//! out, with what happened and how to fix a failure (team admins).
//!
//! - `GET /api/deliveries` lists them with filters and a health summary.
//! - `GET /api/deliveries/:id` is one delivery: headers (secrets removed), a
//!   payload preview, the attempts, the reason in plain words and the fix.
//! - `POST /api/deliveries/:id/redeliver` asks for it again where that's safe.
//!
//! Records come from `common::deliveries` (GitHub events, relay forwards,
//! chat notifications, email) plus two existing logs mapped into the same
//! shape: alert events (`ALERT_EVENT#`) and the Jira event log. On a hybrid
//! install, CoderHelm's relay and broker records for it are fetched from the
//! hosted side too, so a forward that never arrived still shows.

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use common::deliveries::{self, Delivery, Direction, Filter, Status};
use serde_json::{json, Value};
use tracing::warn;

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// Deliveries that reached no team (a bad signature, an unlinked
/// installation). Shown to a self-hosted install's admins.
pub const UNROUTED: &str = "DELIVERIES#UNROUTED";

fn admins_only(c: StatusCode) -> (StatusCode, Json<Value>) {
    fail(c, "Admins only.")
}

fn fail(code: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({ "error": message })))
}

/// Records a delivery; on a hybrid install that shares delivery data, a
/// failure's metadata also goes to CoderHelm support.
pub async fn save(state: &AppState, pk: &str, d: &Delivery) {
    deliveries::record(&state.dynamo, &state.config.settings_table_name, pk, d).await;
    if d.status == Status::Failed {
        super::hybrid_install::share_failure(state, d).await;
    }
}

/// Records one email send (the recipient's domain only, never the address).
pub async fn record_email(
    state: &AppState,
    pk: &str,
    kind: &str,
    to: &str,
    result: Result<(), String>,
) {
    save(state, pk, &deliveries::email(kind, to, &result)).await;
}

fn alert_to_delivery(item: &std::collections::HashMap<String, AttributeValue>) -> Option<Delivery> {
    let g = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let id = g("id");
    if id.is_empty() {
        return None;
    }
    let mut d = Delivery::new(
        Direction::Inbound,
        "alerts",
        &format!("alert.{}", g("kind")),
        g("topic_arn").rsplit(':').next().unwrap_or(""),
    );
    d.id = format!("alert:{id}");
    d.created_at = g("received_at");
    d.repo = g("repo");
    d.outcome = g("title");
    let outcome = g("outcome");
    d.status = match outcome.as_str() {
        "error" => Status::Failed,
        "duplicate" | "ignored" | "muted" => Status::Ignored,
        _ => Status::Handled,
    };
    if d.status == Status::Failed {
        d.reason = "handler_error".into();
    }
    Some(d)
}

fn jira_to_delivery(v: &Value) -> Option<Delivery> {
    let s = |k: &str| v[k].as_str().unwrap_or("").to_string();
    let id = s("event_id");
    if id.is_empty() {
        return None;
    }
    let mut d = Delivery::new(
        Direction::Inbound,
        "jira",
        &s("event_type"),
        &s("ticket_key"),
    );
    d.id = format!("jira:{id}");
    d.created_at = s("created_at");
    d.repo = s("repo");
    d.outcome = s("title");
    let st = s("status");
    (d.status, d.reason) = match st.as_str() {
        "signature_rejected" => (Status::Failed, "jira_signature".into()),
        "token_limit" => (Status::Failed, "jira_limit".into()),
        "duplicate" | "unchanged" => (Status::Ignored, String::new()),
        _ => (Status::Handled, String::new()),
    };
    Some(d)
}

async fn alerts(state: &AppState, team: &str) -> Vec<Delivery> {
    let r = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", AttributeValue::S(team.into()))
        .expression_attribute_values(":p", AttributeValue::S("ALERT_EVENT#".into()))
        .scan_index_forward(false)
        .limit(100)
        .send()
        .await;
    r.map(|r| r.items().iter().filter_map(alert_to_delivery).collect())
        .unwrap_or_default()
}

async fn jira(state: &AppState, team: &str) -> Vec<Delivery> {
    super::jira_webhook::list_jira_events(state, team, 100)
        .await
        .map(|v| v.iter().filter_map(jira_to_delivery).collect())
        .unwrap_or_default()
}

/// CoderHelm's relay and broker records for this install (hybrid only).
async fn relayed(state: &AppState) -> Vec<Delivery> {
    let Some(creds) = common::hybrid::client::credentials() else {
        return vec![];
    };
    match common::hybrid::client::call(&state.http, &creds, "GET", "/hybrid/deliveries", None).await
    {
        Ok(v) => serde_json::from_value::<Vec<Delivery>>(v["deliveries"].clone())
            .unwrap_or_default()
            .into_iter()
            .map(|mut d| {
                d.id = format!("relay:{}", d.id);
                d
            })
            .collect(),
        Err(e) => {
            warn!(error = %e, "Could not read relay deliveries from CoderHelm");
            vec![]
        }
    }
}

async fn everything(state: &AppState, claims: &Claims, f: &Filter) -> Vec<Delivery> {
    let table = &state.config.settings_table_name;
    let own_unrouted = super::setup_checks::self_hosted();
    let (own, unrouted, a, j, r) = tokio::join!(
        deliveries::list(&state.dynamo, table, &claims.team_id, f, 500),
        async {
            if own_unrouted {
                deliveries::list(&state.dynamo, table, UNROUTED, f, 200)
                    .await
                    .unwrap_or_default()
            } else {
                vec![]
            }
        },
        alerts(state, &claims.team_id),
        jira(state, &claims.team_id),
        relayed(state),
    );
    let mut all: Vec<Delivery> = own.unwrap_or_default();
    all.extend(unrouted);
    all.extend(a.into_iter().chain(j).chain(r).filter(|d| f.matches(d)));
    all.sort_by(|x, y| y.created_at.cmp(&x.created_at));
    all
}

/// A list row: no headers or payload (those are in the detail).
fn row(d: &Delivery) -> Value {
    json!({
        "id": d.id,
        "direction": d.direction,
        "source": d.source,
        "event": d.event,
        "target": d.target,
        "status": d.status,
        "outcome": d.outcome,
        "reason": d.reason,
        "external_id": d.external_id,
        "repo": d.repo,
        "pr": d.pr,
        "created_at": d.created_at,
        "duration_ms": d.duration_ms,
        "attempts": d.attempts.len().max(1),
        "next_retry_at": d.next_retry_at,
    })
}

#[derive(serde::Deserialize, Default)]
pub struct ListQuery {
    source: Option<String>,
    status: Option<String>,
    since: Option<String>,
    q: Option<String>,
    limit: Option<usize>,
}

fn sharing_view() -> Value {
    let l = super::hybrid_install::cached_local();
    json!({
        "available": common::hybrid::client::credentials().is_some(),
        "enabled": l.as_ref().is_some_and(|l| l.sharing),
        "payloads": l.as_ref().is_some_and(|l| l.share_payloads),
    })
}

/// GET /api/deliveries?source=&status=&since=&q=&limit=
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<ListQuery>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let f = Filter {
        source: q.source,
        status: q.status,
        since: q.since,
        q: q.q,
        endpoint: None,
    };
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let all = everything(&state, &claims, &f).await;
    // The summary always covers the last 24 hours, whatever the filters say.
    let day_ago = (chrono::Utc::now() - chrono::Duration::hours(24))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let recent = if f.source.is_none() && f.status.is_none() && f.q.is_none() && f.since.is_none() {
        all.clone()
    } else {
        everything(&state, &claims, &Filter::default()).await
    };
    let summary = deliveries::summarize(&recent, &day_ago);
    let mut sources: Vec<&str> = recent.iter().map(|d| d.source.as_str()).collect();
    sources.sort();
    sources.dedup();
    Ok(Json(json!({
        "deliveries": all.iter().take(limit).map(row).collect::<Vec<_>>(),
        "summary": summary,
        "sources": sources,
        "retention_days": deliveries::RETENTION_DAYS,
        "sharing": sharing_view(),
        "hybrid": common::hybrid::client::credentials().is_some(),
    })))
}

/// GET /api/deliveries/:id
pub async fn get(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let found = if id.contains(':') {
        everything(&state, &claims, &Filter::default())
            .await
            .into_iter()
            .find(|d| d.id == id)
    } else {
        let table = &state.config.settings_table_name;
        match deliveries::get(&state.dynamo, table, &claims.team_id, &id).await {
            Ok(Some(d)) => Some(d),
            _ if super::setup_checks::self_hosted() => {
                deliveries::get(&state.dynamo, table, UNROUTED, &id)
                    .await
                    .ok()
                    .flatten()
            }
            _ => None,
        }
    };
    let d = found.ok_or_else(|| {
        fail(
            StatusCode::NOT_FOUND,
            "No such delivery (they're kept 14 days).",
        )
    })?;
    let reason = (!d.reason.is_empty()).then(|| deliveries::reason(&d.reason));
    let redeliver = redeliver_kind(&d);
    Ok(Json(
        json!({ "delivery": d, "reason": reason, "can_redeliver": redeliver.is_some() }),
    ))
}

/// Which redelivery this record allows, if any.
fn redeliver_kind(d: &Delivery) -> Option<deliveries::Redeliver> {
    // GitHub's and a webhook's deliveries can be sent again even when they
    // went fine (the receiver asks for it); the others only when they failed.
    if d.status.is_ok() && d.source != "github" && d.source != "webhook" {
        return None;
    }
    match (d.source.as_str(), d.redeliver) {
        ("github", Some(deliveries::Redeliver::Github)) if !d.external_id.is_empty() => {
            Some(deliveries::Redeliver::Github)
        }
        ("relay", Some(deliveries::Redeliver::Relay)) if d.id.starts_with("relay:") => {
            Some(deliveries::Redeliver::Relay)
        }
        ("webhook", Some(deliveries::Redeliver::Webhook))
            if !d.endpoint_id.is_empty() && !d.external_id.is_empty() =>
        {
            Some(deliveries::Redeliver::Webhook)
        }
        _ => None,
    }
}

/// Finds GitHub's numeric delivery id for its GUID (the last 300 deliveries).
pub fn find_hook_delivery(page: &Value, guid: &str) -> Option<u64> {
    page.as_array()?
        .iter()
        .find(|x| x["guid"].as_str() == Some(guid))
        .and_then(|x| x["id"].as_u64())
}

/// POST /api/deliveries/:id/redeliver
pub async fn redeliver(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let Json(v) = get(
        State(state.clone()),
        Extension(claims.clone()),
        Path(id.clone()),
    )
    .await?;
    let d: Delivery = serde_json::from_value(v["delivery"].clone())
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "Unreadable record."))?;
    match redeliver_kind(&d) {
        Some(deliveries::Redeliver::Webhook) => {
            let d = super::webhooks::resend_record(&state, &claims.team_id, d).await?;
            let ok = d.status.is_ok();
            Ok(Json(json!({
                "ok": ok,
                "message": if ok { "Sent again and delivered." } else { "Sent again; the endpoint didn't take it. The attempt is on the record." },
            })))
        }
        Some(deliveries::Redeliver::Relay) => {
            let creds = common::hybrid::client::credentials().ok_or_else(|| {
                fail(
                    StatusCode::CONFLICT,
                    "This install isn't registered with CoderHelm.",
                )
            })?;
            let raw = d.id.trim_start_matches("relay:");
            common::hybrid::client::call(
                &state.http,
                &creds,
                "POST",
                &format!("/hybrid/deliveries/{raw}/redeliver"),
                Some(&json!({})),
            )
            .await
            .map_err(|e| fail(StatusCode::BAD_GATEWAY, &e.message))?;
            Ok(Json(
                json!({ "ok": true, "message": "Forwarded again. The attempt is on the record." }),
            ))
        }
        Some(deliveries::Redeliver::Github) => {
            if state.secrets.github_private_key.trim().is_empty() {
                return Err(fail(StatusCode::CONFLICT, "This install uses CoderHelm's GitHub App: redeliver the matching relay record instead (source CoderHelm relay)."));
            }
            let jwt = crate::auth::github_app::app_jwt(&state)
                .map_err(|_| fail(StatusCode::CONFLICT, "The GitHub App key can't be read."))?;
            let gh = |url: String| {
                state
                    .http
                    .get(url)
                    .header("Authorization", format!("Bearer {jwt}"))
                    .header("Accept", "application/vnd.github+json")
                    .header("User-Agent", "Coderhelm-bot")
                    .send()
            };
            let mut found = None;
            let mut cursor = String::new();
            for _ in 0..3 {
                let url =
                    format!("https://api.github.com/app/hook/deliveries?per_page=100{cursor}");
                let Ok(r) = gh(url).await else { break };
                let next = r
                    .headers()
                    .get("link")
                    .and_then(|l| l.to_str().ok())
                    .and_then(|l| l.split(',').find(|p| p.contains("rel=\"next\"")))
                    .and_then(|p| p.split("cursor=").nth(1))
                    .map(|c| c.split(['&', '>']).next().unwrap_or("").to_string());
                let page: Value = r.json().await.unwrap_or_default();
                found = find_hook_delivery(&page, &d.external_id);
                match (found, next) {
                    (Some(_), _) | (None, None) => break,
                    (None, Some(c)) => cursor = format!("&cursor={c}"),
                }
            }
            let Some(num) = found else {
                return Err(fail(StatusCode::NOT_FOUND, "GitHub no longer lists this delivery (it keeps about 3 days). Trigger the event again on GitHub."));
            };
            let r = state
                .http
                .post(format!(
                    "https://api.github.com/app/hook/deliveries/{num}/attempts"
                ))
                .header("Authorization", format!("Bearer {jwt}"))
                .header("Accept", "application/vnd.github+json")
                .header("User-Agent", "Coderhelm-bot")
                .send()
                .await
                .map_err(|_| fail(StatusCode::BAD_GATEWAY, "GitHub didn't answer. Try again."))?;
            if !r.status().is_success() {
                return Err(fail(
                    StatusCode::BAD_GATEWAY,
                    "GitHub refused the redelivery.",
                ));
            }
            Ok(Json(
                json!({ "ok": true, "message": "GitHub is sending it again. A new row appears when it arrives." }),
            ))
        }
        _ => Err(fail(
            StatusCode::BAD_REQUEST,
            "This delivery can't be sent again from here. The fix on the delivery says what to do.",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn alert_events_map_into_deliveries() {
        let mut i = HashMap::new();
        for (k, v) in [
            ("id", "01J"),
            ("received_at", "2026-10-25T10:00:00Z"),
            ("kind", "cloudwatch_alarm"),
            ("title", "ALARM: x"),
            ("outcome", "error"),
            ("topic_arn", "arn:aws:sns:us-east-1:1:topic-a"),
        ] {
            i.insert(k.to_string(), AttributeValue::S(v.into()));
        }
        let d = alert_to_delivery(&i).unwrap();
        assert_eq!(d.id, "alert:01J");
        assert_eq!(d.source, "alerts");
        assert_eq!(d.status, Status::Failed);
        assert_eq!(d.target, "topic-a");
        i.insert("outcome".into(), AttributeValue::S("run_started".into()));
        assert_eq!(alert_to_delivery(&i).unwrap().status, Status::Handled);
        assert!(alert_to_delivery(&HashMap::new()).is_none());
    }

    #[test]
    fn jira_events_map_into_deliveries() {
        let d = jira_to_delivery(&json!({"event_id": "e1", "event_type": "issue_updated", "ticket_key": "ABC-1", "status": "signature_rejected", "created_at": "t"})).unwrap();
        assert_eq!(
            (d.id.as_str(), d.status, d.reason.as_str()),
            ("jira:e1", Status::Failed, "jira_signature")
        );
        let d = jira_to_delivery(&json!({"event_id": "e2", "status": "duplicate"})).unwrap();
        assert_eq!(d.status, Status::Ignored);
        assert!(jira_to_delivery(&json!({})).is_none());
    }

    #[test]
    fn redelivery_is_offered_only_where_safe() {
        let mut d = Delivery::new(Direction::Inbound, "github", "push", "TEAM#1");
        d.redeliver = Some(deliveries::Redeliver::Github);
        assert_eq!(redeliver_kind(&d), None, "needs GitHub's delivery id");
        d.external_id = "guid".into();
        assert_eq!(redeliver_kind(&d), Some(deliveries::Redeliver::Github));
        let mut r = Delivery::new(Direction::Outbound, "relay", "push", "ins");
        r.redeliver = Some(deliveries::Redeliver::Relay);
        r.status = Status::Failed;
        assert_eq!(redeliver_kind(&r), None, "only the hosted copy");
        r.id = "relay:01J".into();
        assert_eq!(redeliver_kind(&r), Some(deliveries::Redeliver::Relay));
        r.status = Status::Handled;
        assert_eq!(
            redeliver_kind(&r),
            None,
            "a delivered forward isn't sent twice"
        );
        let e = Delivery::new(Direction::Outbound, "email", "invite", "example.com");
        assert_eq!(redeliver_kind(&e), None);
        let mut w = Delivery::new(
            Direction::Outbound,
            "webhook",
            "run.failed",
            "hooks.example.com",
        );
        w.redeliver = Some(deliveries::Redeliver::Webhook);
        w.status = Status::Handled;
        assert_eq!(redeliver_kind(&w), None, "needs the endpoint and the event");
        w.endpoint_id = "ep_1".into();
        w.external_id = "evt_1".into();
        assert_eq!(
            redeliver_kind(&w),
            Some(deliveries::Redeliver::Webhook),
            "a delivered webhook can be sent again"
        );
    }

    #[test]
    fn github_delivery_ids_are_found_by_guid() {
        let page = json!([{"id": 1, "guid": "a"}, {"id": 2, "guid": "b"}]);
        assert_eq!(find_hook_delivery(&page, "b"), Some(2));
        assert_eq!(find_hook_delivery(&page, "c"), None);
        assert_eq!(find_hook_delivery(&json!({}), "a"), None);
    }
}
