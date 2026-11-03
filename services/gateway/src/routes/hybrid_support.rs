//! Support for self-hosted installs, on both sides (common::support_relay).
//!
//! **Hosted side** (`/hybrid/support/*`, signed as the install): an
//! install's tickets and replies join CoderHelm's support queue under the
//! install's own team key, and the install asks for support's answers since
//! its last poll. Limited per install (the support routes' hourly limits)
//! and per address.
//!
//! **Install side**: a new ticket or reply goes to the outbox and is sent
//! right away, registering the install first when it has no registration
//! (the hybrid registration: an id and a secret, nothing turned on). What
//! isn't sent waits; the worker's `support_sync` sends it again and brings
//! support's answers back.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, StatusCode},
    Json,
};
use common::support_relay::{
    self as relay, Delivery, DynamoLocal, HostedClient, ItemOut, Local, MessageOut, OutboxEntry,
    TicketOut,
};
use serde_json::{json, Value};
use tracing::{info, warn};

use super::hybrid::{self, fail, Install};
use super::support::{self, Message, Ticket};
use crate::middleware::rate_limit::{self, ClientIp};
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// Requests one address may make here in an hour (every install behind it).
const PER_ADDRESS_PER_HOUR: u64 = 120;

fn label(i: &Install) -> String {
    if !i.name.is_empty() {
        common::hybrid::names::hostname(&i.name)
    } else if !i.cloudfront.is_empty() {
        i.cloudfront.clone()
    } else {
        i.id.clone()
    }
}

async fn per_address(state: &AppState, ip: &ClientIp) -> Result<(), (StatusCode, Json<Value>)> {
    rate_limit::allow_shared(state, "hybrid-support", &ip.0, PER_ADDRESS_PER_HOUR, 3600)
        .await
        .map_err(|_| {
            fail(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "Too many support requests from here. Try again in an hour.",
            )
        })
}

fn parse<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, (StatusCode, Json<Value>)> {
    if body.len() > 64 * 1024 {
        return Err(fail(
            StatusCode::PAYLOAD_TOO_LARGE,
            "too_large",
            "That ticket is too large.",
        ));
    }
    serde_json::from_slice(body).map_err(|_| {
        fail(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "Send the ticket's fields.",
        )
    })
}

/// POST /hybrid/support/tickets (signed): a new ticket from an install.
pub async fn tickets(
    State(state): State<Arc<AppState>>,
    ip: ClientIp,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = hybrid::signed(&state, &headers, &method, &uri, &body).await?;
    per_address(&state, &ip).await?;
    let t: TicketOut = parse(&body)?;
    let st = support::store(&state);
    let added = support::intake_ticket(&st, &install.id, &label(&install), &t)
        .await
        .map_err(support::Fail::reply)?;
    let Some((ticket, first)) = added else {
        return Ok(Json(
            json!({ "ok": true, "ticket_id": t.ticket_id, "duplicate": true }),
        ));
    };
    info!(install = %install.id, ticket = %ticket.id, "support ticket from a self-hosted install");
    hybrid::audit(
        &state,
        &install.id,
        "support_ticket",
        json!({ "ticket": ticket.id }),
    )
    .await;
    support::send(
        &state,
        support::mail_new_ticket(&support::inbox(), &ticket, &first.body),
    )
    .await;
    Ok(Json(json!({ "ok": true, "ticket_id": ticket.id })))
}

/// POST /hybrid/support/messages (signed): a person's reply, from an install.
pub async fn messages(
    State(state): State<Arc<AppState>>,
    ip: ClientIp,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = hybrid::signed(&state, &headers, &method, &uri, &body).await?;
    per_address(&state, &ip).await?;
    let m: MessageOut = parse(&body)?;
    let st = support::store(&state);
    let added = support::intake_message(&st, &install.id, &m)
        .await
        .map_err(support::Fail::reply)?;
    if let Some((ticket, msg)) = added {
        support::send(
            &state,
            support::mail_customer_reply(&support::inbox(), &ticket, &msg),
        )
        .await;
    }
    Ok(Json(json!({ "ok": true })))
}

/// POST /hybrid/support/updates (signed) `{since}`: support's answers and
/// status changes on this install's tickets after `since`.
pub async fn updates(
    State(state): State<Arc<AppState>>,
    ip: ClientIp,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = hybrid::signed(&state, &headers, &method, &uri, &body).await?;
    per_address(&state, &ip).await?;
    let b: Value = parse(&body)?;
    let since: String = b["since"].as_str().unwrap_or("").chars().take(40).collect();
    let st = support::store(&state);
    let u = support::updates_for(&st, &install.id, &since)
        .await
        .map_err(support::Fail::reply)?;
    Ok(Json(serde_json::to_value(u).unwrap_or(Value::Null)))
}

// ── Install side ─────────────────────────────────────────────────────────

fn local(state: &AppState) -> DynamoLocal<'_> {
    DynamoLocal {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
    }
}

/// An attached item as it leaves: a run with its error text and step.
async fn item_out(state: &AppState, team_id: &str, i: &support::RelatedItem) -> ItemOut {
    let mut out = ItemOut {
        kind: i.kind.clone(),
        id: i.id.clone(),
        label: i.label.clone(),
        status: i.status.clone(),
        ..Default::default()
    };
    if i.kind == "run" {
        let row = state
            .dynamo
            .get_item()
            .table_name(&state.config.runs_table_name)
            .key(
                "team_id",
                aws_sdk_dynamodb::types::AttributeValue::S(team_id.into()),
            )
            .key(
                "run_id",
                aws_sdk_dynamodb::types::AttributeValue::S(i.id.clone()),
            )
            .projection_expression("#e, #p")
            .expression_attribute_names("#e", "error")
            .expression_attribute_names("#p", "pass")
            .send()
            .await
            .ok()
            .and_then(|r| r.item().cloned());
        if let Some(row) = row {
            let get = |k: &str| {
                row.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            out.error = get("error");
            out.step = get("pass");
        }
    }
    out.clean(support::scrub)
}

async fn items_out(
    state: &AppState,
    team_id: &str,
    items: &[support::RelatedItem],
) -> Vec<ItemOut> {
    let mut v = Vec::new();
    for i in items.iter().take(relay::MAX_ITEMS) {
        v.push(item_out(state, team_id, i).await);
    }
    v
}

fn person(t: &Ticket, m: &Message) -> (String, String) {
    let email = if m.author_email.is_empty() {
        &t.opened_by_email
    } else {
        &m.author_email
    };
    let name = if m.author_name.is_empty() {
        email
    } else {
        &m.author_name
    };
    (relay::clip(name, relay::MAX_NAME), email.to_string())
}

/// A ticket just opened on this install: into the outbox, then sent.
pub async fn ticket_opened(state: &AppState, t: &Ticket, first: &Message) {
    let (name, email) = person(t, first);
    let out = TicketOut {
        ticket_id: t.id.clone(),
        message_id: first.id.clone(),
        subject: t.subject.clone(),
        body: first.body.clone(),
        name,
        email,
        page: t.context.page.clone(),
        priority: t.priority.clone(),
        items: items_out(state, &t.team_id, &t.context.items).await,
        version: std::env::var("CODERHELM_VERSION").unwrap_or_default(),
        created_at: first.created_at.clone(),
    };
    queue(state, OutboxEntry::ticket(&t.team_id, &out)).await;
}

/// A reply on this install's ticket: into the outbox, then sent.
pub async fn reply_added(state: &AppState, t: &Ticket, m: &Message) {
    let (name, email) = person(t, m);
    let out = MessageOut {
        ticket_id: t.id.clone(),
        message_id: m.id.clone(),
        body: m.body.clone(),
        name,
        email,
        items: items_out(state, &t.team_id, &m.attachments).await,
        created_at: m.created_at.clone(),
    };
    queue(state, OutboxEntry::message(&t.team_id, &out)).await;
}

async fn queue(state: &AppState, e: OutboxEntry) {
    let l = local(state);
    let sending = Delivery {
        state: "sending".into(),
        ..Default::default()
    };
    if let Err(err) = l.enqueue(&e).await {
        warn!(error = %err, "support outbox write failed");
        return;
    }
    let _ = l.set_delivery(&e.team_id, &e.ticket_id, &sending).await;
    send_waiting(state).await;
}

/// The install's registration, registering first when it has none. The
/// reason in words when it can't.
async fn credentials(state: &AppState) -> Result<common::hybrid::client::Credentials, String> {
    if let Some(c) = common::hybrid::client::credentials() {
        return Ok(c);
    }
    super::hybrid_install::do_register(state)
        .await
        .map_err(|(_, Json(v))| {
            v["error"]
                .as_str()
                .unwrap_or("CoderHelm refused the registration.")
                .to_string()
        })?;
    common::hybrid::client::credentials()
        .ok_or_else(|| "The install has no registration yet.".to_string())
}

/// Send what's waiting. When the install can't reach CoderHelm at all, each
/// waiting ticket says so (it stays "Sending to CoderHelm").
pub async fn send_waiting(state: &AppState) {
    let l = local(state);
    let creds = match credentials(state).await {
        Ok(c) => c,
        Err(reason) => {
            warn!(reason = %reason, "support: can't reach CoderHelm to send tickets");
            if let Ok(waiting) = l.outbox().await {
                for e in waiting {
                    let _ = l.failed(&e, &reason).await;
                    let d = Delivery {
                        state: "sending".into(),
                        error: reason.clone(),
                        sent_at: String::new(),
                    };
                    let _ = l.set_delivery(&e.team_id, &e.ticket_id, &d).await;
                }
            }
            return;
        }
    };
    let hosted = HostedClient {
        http: &state.http,
        creds,
    };
    match relay::flush(&l, &hosted).await {
        Ok(r) if r.sent + r.failed > 0 => {
            info!(sent = r.sent, failed = r.failed, "support: outbox sent")
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "support: outbox send failed"),
    }
}

static LAST_FETCH: AtomicI64 = AtomicI64::new(0);

/// On the install's ticket pages: send anything waiting and fetch support's
/// answers, at most once a minute (the worker also does it every few minutes).
pub async fn catch_up(state: &AppState) {
    let now = chrono::Utc::now().timestamp();
    if now - LAST_FETCH.load(Ordering::Relaxed) < 60 {
        return;
    }
    LAST_FETCH.store(now, Ordering::Relaxed);
    let l = local(state);
    if l.outbox().await.is_ok_and(|w| !w.is_empty()) {
        send_waiting(state).await;
    }
    let Some(creds) = common::hybrid::client::credentials() else {
        return;
    };
    let hosted = HostedClient {
        http: &state.http,
        creds,
    };
    match relay::poll(&l, &hosted).await {
        Ok(added) => {
            let app = common::public_urls::app_url_from_env();
            for (ticket, u) in added {
                for mail in relay::mails_for(&ticket, &u, app) {
                    support::send_mail(state, mail).await;
                }
            }
        }
        Err(e) => warn!(error = %e, "support: fetching answers failed"),
    }
}
