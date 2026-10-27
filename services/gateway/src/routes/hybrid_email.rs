//! The hosted side of CoderHelm's mail relay (`/hybrid/email*`, CoderHelm's
//! own deployment only; 404 everywhere else).
//!
//! An install that chose "Send email through CoderHelm" signs each request
//! with its registration secret, as for every hybrid call. The rules
//! (templates, recipients, limits, pausing) are in `common::mail`; this file
//! loads what they need and writes what they decide. Records live under the
//! install's `HYBRID#<install>` partition on the settings table: listed
//! addresses as salted hashes (`EMAILTO#`), suppressed ones (`EMAILSUPPRESS#`),
//! used request ids (`EMAILREQ#`, short TTL), counters (`EMAILDAY#`,
//! `EMAILMIN#`), the pause (`EMAILPAUSE`) and delivery records with the
//! template id, a redacted address and the status (3-day TTL). Nothing
//! keeps a rendered email or a parameter.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    Extension, Json,
};
use common::deliveries::{self, Delivery, Direction, Status};
use common::hybrid::relay as hybrid_relay;
use common::mail::{relay, templates};
use serde_json::{json, Value};
use tracing::{info, warn};

use super::hybrid::{audit, fail, master, not_here, operator, signed, Install};
use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn not_admin(c: StatusCode) -> (StatusCode, Json<Value>) {
    fail(c, "forbidden", "Platform admins only.")
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn epoch() -> i64 {
    chrono::Utc::now().timestamp()
}

fn refuse(r: relay::Refusal) -> (StatusCode, Json<Value>) {
    fail(
        StatusCode::from_u16(r.status()).unwrap_or(StatusCode::FORBIDDEN),
        r.code(),
        r.message(),
    )
}

fn unavailable() -> (StatusCode, Json<Value>) {
    fail(
        StatusCode::SERVICE_UNAVAILABLE,
        "store_unavailable",
        "Try again in a minute.",
    )
}

/// The relay's sender (`HYBRID_EMAIL_FROM`), set only on the hosted side.
fn relay_from() -> Option<String> {
    std::env::var("HYBRID_EMAIL_FROM")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| relay::valid_address(v))
}

/// The hosts an install's links may point at: its CloudFront address and
/// its CoderHelm name, both verified.
pub fn own_hosts(i: &Install) -> Vec<String> {
    i.origins()
        .iter()
        .map(|o| o.trim_start_matches("https://").to_string())
        .collect()
}

/// The install's address for the From line and the footer.
pub fn install_host(i: &Install) -> String {
    if i.name.is_empty() {
        i.cloudfront.clone()
    } else {
        common::hybrid::names::hostname(&i.name)
    }
}

fn name_host(i: &Install) -> Option<String> {
    (!i.name.is_empty()).then(|| common::hybrid::names::hostname(&i.name))
}

/// A verified, active install that signed the request, and its relay sender.
async fn verified(
    state: &AppState,
    headers: &HeaderMap,
    method: &Method,
    uri: &Uri,
    body: &[u8],
) -> Result<(Install, String), (StatusCode, Json<Value>)> {
    let install = signed(state, headers, method, uri, body).await?;
    let from = relay_from().ok_or_else(not_here)?;
    if !install.verified() {
        return Err(refuse(relay::Refusal::NotVerified));
    }
    Ok((install, from))
}

/// Remembers a request id once; a second use is refused.
async fn first_use(
    state: &AppState,
    install: &str,
    id: &str,
) -> Result<(), (StatusCode, Json<Value>)> {
    if !relay::valid_request_id(id) {
        return Err(refuse(relay::Refusal::BadRequest));
    }
    let put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(&hybrid_relay::install_pk(install)))
        .item("sk", s(&relay::request_sk(id)))
        .item("ttl", n(epoch() + relay::REPLAY_TTL_SECS))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await;
    match put {
        Ok(_) => Ok(()),
        Err(e) => {
            let text = aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string();
            if text.contains("ConditionalCheckFailed") {
                Err(refuse(relay::Refusal::Replayed))
            } else {
                warn!(error = %text, "email relay: replay record failed");
                Err(unavailable())
            }
        }
    }
}

async fn get(
    state: &AppState,
    install: &str,
    sk: &str,
) -> Result<Option<HashMap<String, AttributeValue>>, (StatusCode, Json<Value>)> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&hybrid_relay::install_pk(install)))
        .key("sk", s(sk))
        .consistent_read(true)
        .send()
        .await
        .map(|r| r.item().cloned())
        .map_err(|e| {
            warn!(error = %e, "email relay: read failed");
            unavailable()
        })
}

fn kind_of(item: Option<&HashMap<String, AttributeValue>>) -> Option<relay::RecipientKind> {
    item?
        .get("kind")
        .and_then(|v| v.as_s().ok())
        .and_then(|k| relay::RecipientKind::parse(k))
}

/// Adds one to a counter unless it already reached `max`.
async fn count_up(
    state: &AppState,
    install: &str,
    sk: &str,
    field: &str,
    by: u64,
    max: Option<u64>,
    ttl_secs: i64,
) -> Result<bool, (StatusCode, Json<Value>)> {
    let mut u = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&hybrid_relay::install_pk(install)))
        .key("sk", s(sk))
        .update_expression("ADD #f :by SET #t = if_not_exists(#t, :ttl)")
        .expression_attribute_names("#f", field)
        .expression_attribute_names("#t", "ttl")
        .expression_attribute_values(":by", AttributeValue::N(by.to_string()))
        .expression_attribute_values(":ttl", n(epoch() + ttl_secs));
    if let Some(max) = max {
        u = u
            .condition_expression("attribute_not_exists(#f) OR #f <= :room")
            .expression_attribute_values(
                ":room",
                AttributeValue::N(max.saturating_sub(by).to_string()),
            );
    }
    match u.send().await {
        Ok(_) => Ok(true),
        Err(e) => {
            let text = aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string();
            if text.contains("ConditionalCheckFailed") {
                Ok(false)
            } else {
                warn!(error = %text, "email relay: counter failed");
                Err(unavailable())
            }
        }
    }
}

fn today() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

fn this_minute() -> String {
    chrono::Utc::now().format("%Y%m%d%H%M").to_string()
}

async fn record(state: &AppState, install: &str, d: &Delivery) {
    // Delivery metadata only, kept a short while.
    let mut item = deliveries::to_item(&hybrid_relay::install_pk(install), d, epoch());
    item.insert("ttl".into(), n(epoch() + relay::DELIVERY_TTL_SECS));
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(item))
        .send()
        .await
        .map_err(|e| warn!(error = %e, "email relay: delivery record failed"));
}

/// The delivery record for one relayed email: template id, redacted
/// address, status. Never a parameter.
pub fn relay_delivery(template: &str, to: &str, outcome: Result<(), &str>) -> Delivery {
    let mut d = Delivery::new(
        Direction::Outbound,
        "email_relay",
        &format!("email.{template}"),
        &relay::redact(to),
    );
    match outcome {
        Ok(()) => d.finish(Status::Handled, "Sent", ""),
        Err(code) => d.finish(Status::Failed, "Not sent", code),
    }
    d
}

/// POST /hybrid/email (signed): one email from a template.
pub async fn send(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let (install, from) = verified(&state, &headers, &method, &uri, &body).await?;
    let req: relay::SendRequest =
        serde_json::from_slice(&body).map_err(|_| refuse(relay::Refusal::BadRequest))?;
    let fail_with = |code: &str| relay_delivery(&req.template, &req.to, Err(code));
    let hosts = relay::link_hosts(
        &own_hosts(&install),
        custom_domain(&state, &install.id).await?.as_ref(),
        epoch(),
    );
    let (template, values) = match templates::check(&req.template, &req.params, &hosts, true) {
        Ok(v) => v,
        Err(r) => {
            record(&state, &install.id, &fail_with(r.code())).await;
            return Err(fail(StatusCode::BAD_REQUEST, r.code(), &r.message()));
        }
    };
    first_use(&state, &install.id, &req.request_id).await?;
    let hash = relay::recipient_hash(&install.id, &req.to);
    let listing = relay::Listing {
        recipient: kind_of(
            get(&state, &install.id, &relay::recipient_sk(&hash))
                .await?
                .as_ref(),
        ),
        suppressed: get(&state, &install.id, &relay::suppressed_sk(&hash))
            .await?
            .is_some(),
    };
    let paused = get(&state, &install.id, relay::PAUSE_SK).await?.is_some();
    if let Err(r) = relay::may_send(&req.to, &listing, paused) {
        record(&state, &install.id, &fail_with(r.code())).await;
        return Err(refuse(r));
    }
    if !count_up(
        &state,
        &install.id,
        &relay::minute_sk(&this_minute()),
        "sent",
        1,
        Some(relay::PER_MINUTE),
        300,
    )
    .await?
    {
        record(
            &state,
            &install.id,
            &fail_with(relay::Refusal::RateLimited.code()),
        )
        .await;
        return Err(refuse(relay::Refusal::RateLimited));
    }
    if !count_up(
        &state,
        &install.id,
        &relay::day_sk(&today()),
        "sent",
        1,
        Some(relay::PER_DAY),
        40 * 86_400,
    )
    .await?
    {
        record(
            &state,
            &install.id,
            &fail_with(relay::Refusal::QuotaReached.code()),
        )
        .await;
        return Err(refuse(relay::Refusal::QuotaReached));
    }
    // The footer names an install admin only when they are a listed admin.
    let contact = match req.contact.as_deref().filter(|c| relay::valid_address(c)) {
        Some(c) => {
            let k = kind_of(
                get(
                    &state,
                    &install.id,
                    &relay::recipient_sk(&relay::recipient_hash(&install.id, c)),
                )
                .await?
                .as_ref(),
            );
            (k == Some(relay::RecipientKind::Admin)).then(|| relay::normalize(c))
        }
        None => None,
    };
    let host = install_host(&install);
    let email = templates::render(template, &values, &host, contact.as_deref());
    let display = relay::display_name(name_host(&install).as_deref(), &install.cloudfront);
    let sent = send_ses(
        &state,
        &from,
        &display,
        &req.to,
        &email,
        &install.id,
        template.id,
    )
    .await;
    let d = relay_delivery(
        template.id,
        &req.to,
        sent.as_ref().map(|_| ()).map_err(|_| "email_failed"),
    );
    record(&state, &install.id, &d).await;
    match sent {
        Ok(()) => {
            info!(install = %install.id, template = template.id, "email relay: sent");
            Ok(Json(json!({ "ok": true, "delivery_id": d.id })))
        }
        Err(e) => {
            warn!(install = %install.id, template = template.id, error = %e, "email relay: SES refused");
            Err(fail(
                StatusCode::BAD_GATEWAY,
                "email_failed",
                "CoderHelm's mail service couldn't send it. Try again later.",
            ))
        }
    }
}

/// Sends a written email from the relay's sender, tagged with the install
/// so bounces and complaints find their way back.
pub async fn send_ses(
    state: &AppState,
    from: &str,
    display: &str,
    to: &str,
    email: &templates::Rendered,
    install_id: &str,
    template: &str,
) -> Result<(), String> {
    use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message, MessageTag};
    let c = |v: &str| {
        Content::builder()
            .data(v)
            .charset("UTF-8")
            .build()
            .map_err(|e| e.to_string())
    };
    let message = Message::builder()
        .subject(c(&email.subject)?)
        .body(
            Body::builder()
                .text(c(&email.text)?)
                .html(c(&email.html)?)
                .build(),
        )
        .build();
    let tag = |k: &str, v: &str| {
        MessageTag::builder()
            .name(k)
            .value(v)
            .build()
            .map_err(|e| e.to_string())
    };
    let mut req = state
        .ses
        .send_email()
        .from_email_address(relay::from_header(display, from))
        .destination(Destination::builder().to_addresses(to).build())
        .content(EmailContent::builder().simple(message).build())
        .email_tags(tag(relay::INSTALL_TAG, install_id)?)
        .email_tags(tag(relay::TEMPLATE_TAG, template)?);
    if let Ok(set) = std::env::var("HYBRID_EMAIL_CONFIG_SET") {
        if !set.trim().is_empty() {
            req = req.configuration_set_name(set.trim());
        }
    }
    req.send()
        .await
        .map(|_| ())
        .map_err(|e| aws_sdk_sesv2::error::DisplayErrorContext(&e).to_string())
}

/// POST /hybrid/email/recipients (signed): list or unlist addresses.
pub async fn recipients(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let (install, _) = verified(&state, &headers, &method, &uri, &body).await?;
    let req: relay::RecipientsRequest =
        serde_json::from_slice(&body).map_err(|_| refuse(relay::Refusal::BadRequest))?;
    first_use(&state, &install.id, &req.request_id).await?;
    let by_hash = relay::recipient_hash(&install.id, &req.by);
    let by_kind = kind_of(
        get(&state, &install.id, &relay::recipient_sk(&by_hash))
            .await?
            .as_ref(),
    );
    let list_meta = get(&state, &install.id, "EMAILLIST").await?;
    let admins_listed = list_meta
        .as_ref()
        .and_then(|i| i.get("admins"))
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0)
        > 0;
    let day = get(&state, &install.id, &relay::day_sk(&today())).await?;
    let new_today = day
        .as_ref()
        .and_then(|i| i.get("new_recipients"))
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    relay::may_list(&req, by_kind, admins_listed, new_today).map_err(refuse)?;

    let cipher = common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: &state.config.secrets_key_id,
    };
    let (mut added, mut admins) = (0u64, 0i64);
    for r in &req.add {
        let hash = relay::recipient_hash(&install.id, &r.email);
        let existing = kind_of(
            get(&state, &install.id, &relay::recipient_sk(&hash))
                .await?
                .as_ref(),
        );
        // An admin stays an admin; an invite becomes a member.
        let kind = match (existing, r.kind) {
            (Some(relay::RecipientKind::Admin), _) => relay::RecipientKind::Admin,
            (_, k) => k,
        };
        let mut put = state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item("pk", s(&hybrid_relay::install_pk(&install.id)))
            .item("sk", s(&relay::recipient_sk(&hash)))
            .item("kind", s(kind.as_str()))
            .item("added_at", s(&chrono::Utc::now().to_rfc3339()));
        if kind == relay::RecipientKind::Admin {
            // Admins' addresses are kept sealed, only to tell them when
            // the relay pauses.
            if let Ok(sealed) = seal_admin(&cipher, &install.id, &r.email).await {
                put = put.item("sealed", s(&sealed));
            }
            if existing != Some(relay::RecipientKind::Admin) {
                admins += 1;
            }
        }
        put.send().await.map_err(|e| {
            warn!(error = %e, "email relay: list write failed");
            unavailable()
        })?;
        if existing.is_none() {
            added += 1;
        }
    }
    let mut removed = 0u64;
    for e in &req.remove {
        let hash = relay::recipient_hash(&install.id, e);
        if kind_of(
            get(&state, &install.id, &relay::recipient_sk(&hash))
                .await?
                .as_ref(),
        ) == Some(relay::RecipientKind::Admin)
        {
            admins -= 1;
        }
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", s(&hybrid_relay::install_pk(&install.id)))
            .key("sk", s(&relay::recipient_sk(&hash)))
            .send()
            .await;
        removed += 1;
    }
    if added > 0 {
        count_up(
            &state,
            &install.id,
            &relay::day_sk(&today()),
            "new_recipients",
            added,
            None,
            40 * 86_400,
        )
        .await?;
    }
    if admins != 0 {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", s(&hybrid_relay::install_pk(&install.id)))
            .key("sk", s("EMAILLIST"))
            .update_expression("ADD admins :d SET opted_in_at = if_not_exists(opted_in_at, :now)")
            .expression_attribute_values(":d", n(admins))
            .expression_attribute_values(":now", s(&chrono::Utc::now().to_rfc3339()))
            .send()
            .await;
    }
    // Counts and a hash of who stood behind it; never an address.
    audit(
        &state,
        &install.id,
        "email_recipients",
        json!({ "by": &by_hash[..16], "added": added, "removed": removed }),
    )
    .await;
    Ok(Json(
        json!({ "ok": true, "added": added, "removed": removed }),
    ))
}

async fn seal_admin(
    cipher: &dyn common::mcp_credentials::Cipher,
    install: &str,
    email: &str,
) -> Result<String, String> {
    use base64::Engine;
    let ctx = relay::admin_context(install);
    let blob = cipher
        .encrypt(ctx, relay::normalize(email).as_bytes())
        .await?;
    Ok(base64::engine::general_purpose::STANDARD.encode(blob))
}

/// The install's own domain, as last asked for and proved.
async fn custom_domain(
    state: &AppState,
    install: &str,
) -> Result<Option<relay::CustomDomain>, (StatusCode, Json<Value>)> {
    Ok(get(state, install, relay::DOMAIN_SK).await?.and_then(|i| {
        let hostname = i.get("hostname")?.as_s().ok()?.clone();
        let verified_at = i
            .get("verified_at")
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse().ok());
        Some(relay::CustomDomain {
            hostname,
            verified_at,
        })
    }))
}

/// Whether the install answers its own proof on `host` over HTTPS: the
/// name resolves to it and its certificate is valid. Never a redirect, at
/// most a small answer.
async fn proves_on(master: &str, install: &Install, host: &str) -> bool {
    let challenge = common::hybrid::broker::new_handle("ch");
    let url = format!("https://{host}/webhooks/relay/proof?challenge={challenge}");
    let Ok(r) = hybrid_relay::install_client()
        .get(&url)
        .timeout(std::time::Duration::from_secs(6))
        .send()
        .await
    else {
        return false;
    };
    if !r.status().is_success() {
        return false;
    }
    let v: Value = serde_json::from_slice(&hybrid_relay::read_capped(r).await).unwrap_or_default();
    v["install_id"].as_str() == Some(install.id.as_str())
        && common::hybrid::sign::check_proof(
            &common::hybrid::sign::install_secret(master, &install.id),
            &challenge,
            v["proof"].as_str().unwrap_or(""),
        )
}

/// POST /hybrid/email/domain (signed) `{hostname}`: let relayed emails link
/// to the install's own domain. The relay proves it first, over HTTPS on
/// that name, the same proof as the CloudFront address; until then (and
/// once the proof is a week old) links there are refused.
pub async fn domain(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let (install, _) = verified(&state, &headers, &method, &uri, &body).await?;
    let master = master(&state).ok_or_else(not_here)?.to_string();
    crate::middleware::rate_limit::allow(&headers, "hybrid-email-domain", 20, 3600)
        .map_err(|_| refuse(relay::Refusal::RateLimited))?;
    let host = serde_json::from_slice::<Value>(&body).unwrap_or_default()["hostname"]
        .as_str()
        .unwrap_or("")
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if !relay::custom_domain_ok(&host) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "bad_domain",
            "Send the install's own domain, such as coderhelm.example.com.",
        ));
    }
    let ok = proves_on(&master, &install, &host).await;
    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(&hybrid_relay::install_pk(&install.id)))
        .item("sk", s(relay::DOMAIN_SK))
        .item("hostname", s(&host));
    if ok {
        put = put.item("verified_at", n(epoch()));
    }
    put.send().await.map_err(|e| {
        warn!(error = %e, "email relay: domain write failed");
        unavailable()
    })?;
    audit(
        &state,
        &install.id,
        "email_domain",
        json!({ "hostname": host, "verified": ok }),
    )
    .await;
    if !ok {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "domain_not_proved",
            "CoderHelm couldn't reach this install over HTTPS on that domain. Check its DNS records and certificate in Settings, System, Domain.",
        ));
    }
    Ok(Json(json!({ "ok": true, "hostname": host })))
}

/// GET /hybrid/email/status (signed): what the install's Settings, Emails
/// and Setup checks show.
pub async fn status(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    relay_from().ok_or_else(not_here)?;
    let pause = get(&state, &install.id, relay::PAUSE_SK).await?;
    let day = get(&state, &install.id, &relay::day_sk(&today())).await?;
    let num = |i: &Option<HashMap<String, AttributeValue>>, k: &str| {
        i.as_ref()
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let text = |i: &Option<HashMap<String, AttributeValue>>, k: &str| {
        i.as_ref()
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    Ok(Json(json!({
        "verified": install.verified(),
        "paused": pause.as_ref().map(|_| json!({ "reason": text(&pause, "reason"), "at": text(&pause, "at") })),
        "sent_today": num(&day, "sent"),
        "quota": relay::PER_DAY,
        "per_minute": relay::PER_MINUTE,
        "new_recipients_today": num(&day, "new_recipients"),
        "new_recipients_per_day": relay::NEW_RECIPIENTS_PER_DAY,
        "from": relay_from(),
        "display_name": relay::display_name(name_host(&install).as_deref(), &install.cloudfront),
    })))
}

/// POST /api/admin/hybrid/:install_id/email/pause `{paused, reason}`:
/// pause or resume the mail relay for one install (platform admins).
pub async fn admin_pause(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(not_admin)?;
    operator(&claims)?;
    master(&state).ok_or_else(not_here)?;
    if !common::hybrid::sign::is_install_id(&id) {
        return Err(fail(StatusCode::NOT_FOUND, "not_found", "No such install."));
    }
    let paused = body["paused"].as_bool().unwrap_or(true);
    let reason = body["reason"]
        .as_str()
        .unwrap_or("")
        .chars()
        .take(200)
        .collect::<String>();
    let table = &state.config.settings_table_name;
    let pk = s(&hybrid_relay::install_pk(&id));
    let r = if paused {
        state
            .dynamo
            .put_item()
            .table_name(table)
            .item("pk", pk)
            .item("sk", s(relay::PAUSE_SK))
            .item("reason", s("operator"))
            .item("at", s(&chrono::Utc::now().to_rfc3339()))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    } else {
        state
            .dynamo
            .delete_item()
            .table_name(table)
            .key("pk", pk)
            .key("sk", s(relay::PAUSE_SK))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    r.map_err(|e| {
        warn!(error = %e, "email relay: pause write failed");
        unavailable()
    })?;
    audit(
        &state,
        &id,
        if paused {
            "email_pause"
        } else {
            "email_resume"
        },
        json!({ "by": claims.email, "reason": reason }),
    )
    .await;
    Ok(Json(json!({ "ok": true, "paused": paused })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install(name: &str) -> Install {
        Install {
            id: "ins_abcdefghijklmnopqrstuvwx".into(),
            status: "active".into(),
            cloudfront: "d1abcdef234567.cloudfront.net".into(),
            verified_at: "2026-10-25T00:00:00Z".into(),
            name: name.into(),
            ..Default::default()
        }
    }

    #[test]
    fn links_may_point_only_at_the_installs_own_hosts() {
        let i = install("acme");
        assert_eq!(
            own_hosts(&i),
            vec!["d1abcdef234567.cloudfront.net", "acme.coderhelm.com"]
        );
        assert_eq!(install_host(&i), "acme.coderhelm.com");
        assert_eq!(install_host(&install("")), "d1abcdef234567.cloudfront.net");
        assert_eq!(
            own_hosts(&install("")),
            vec!["d1abcdef234567.cloudfront.net"]
        );
    }

    #[test]
    fn a_relay_delivery_keeps_no_address_and_no_parameter() {
        let d = relay_delivery("plugin_approval", "Dev@Example.com", Ok(()));
        let all = serde_json::to_string(&d).unwrap();
        assert_eq!(d.target, "d***@example.com");
        assert!(!all.contains("Dev@"));
        assert!(d.payload_preview.is_empty());
        let f = relay_delivery("welcome", "x@y.com", Err("recipient_not_listed"));
        assert_eq!(f.reason, "recipient_not_listed");
    }

    #[test]
    fn admin_addresses_are_sealed_per_install() {
        let a = relay::admin_context("ins_a");
        assert_eq!(a["install"], "ins_a");
        assert_ne!(a, relay::admin_context("ins_b"));
    }
}
