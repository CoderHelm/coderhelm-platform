//! The hosted side of hybrid self-hosting (`/hybrid/*`, CoderHelm's own
//! deployment only).
//!
//! A self-hosted install registers here and then, signed with its
//! registration secret (common::hybrid::sign), can:
//!
//! - claim `{name}.coderhelm.com` and have its two DNS records written into
//!   the hosted coderhelm.com zone (the certificate stays in its account);
//! - link installations of CoderHelm's GitHub App, receive their events
//!   through the relay ([`relay_github`]) and get installation tokens from
//!   the broker, only for what it was linked to;
//! - sign in to Notion, Confluence and Google Drive through CoderHelm's OAuth
//!   apps: this side acts as a standard OAuth authorize and token endpoint
//!   toward the install, and keeps the provider sign-in sealed (KMS) under
//!   the install's own partition;
//! - share failed-delivery metadata with support, when it opted in.
//!
//! Everything here answers 404 on a deployment without the hybrid master
//! key, so self-hosted installs never serve it. Records live on the settings
//! table: `HYBRID#<install>` (the install, its links, its relay and broker
//! deliveries), `HYBRIDNAME#<name>`, `HYBRIDGH#<installation>`, and
//! `HYBRIDAUDIT` (who did what, for support and revocation).

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Redirect, Response},
    Extension, Json,
};
use common::deliveries::{self, Delivery, Direction, Status};
use common::hybrid::{broker, names, relay, sign};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::middleware::rate_limit::{self, ClientIp};
use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

pub(crate) fn fail(code: StatusCode, reason: &str, message: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({ "code": reason, "error": message })))
}

pub(crate) fn not_here() -> (StatusCode, Json<Value>) {
    fail(StatusCode::NOT_FOUND, "not_found", "Not found.")
}

pub(crate) fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

pub(crate) fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

pub(crate) fn epoch() -> i64 {
    chrono::Utc::now().timestamp()
}

/// The master key, when this deployment is the hosted side.
pub fn master(state: &AppState) -> Option<&str> {
    state
        .secrets
        .hybrid_master_key
        .as_deref()
        .filter(|k| common::hybrid::is_hosted_side(Some(k)))
}

// ── Records ──────────────────────────────────────────────────────────────

/// One registered install, as this side keeps it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Install {
    pub id: String,
    pub status: String,
    /// The install's CloudFront hostname (`d1234.cloudfront.net`), where the
    /// relay and the ownership proof go.
    pub cloudfront: String,
    pub verified_at: String,
    pub name: String,
    pub created_at: String,
    pub version: String,
    pub sharing: bool,
    pub share_payloads: bool,
    pub integrations: Vec<String>,
    /// Name claims and releases in the last day (epoch seconds), for the limit.
    pub name_changes: Vec<i64>,
}

impl Install {
    fn from_item(i: &HashMap<String, AttributeValue>) -> Option<Self> {
        let g = |k: &str| {
            i.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        let b = |k: &str| {
            i.get(k)
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false)
        };
        let list = |k: &str| {
            i.get(k)
                .and_then(|v| v.as_ss().ok())
                .cloned()
                .unwrap_or_default()
        };
        Some(Self {
            id: i.get("install_id")?.as_s().ok()?.clone(),
            status: g("status"),
            cloudfront: g("cloudfront"),
            verified_at: g("verified_at"),
            name: g("name"),
            created_at: g("created_at"),
            version: g("version"),
            sharing: b("sharing"),
            share_payloads: b("share_payloads"),
            integrations: list("integrations"),
            name_changes: g("name_changes")
                .split(',')
                .filter_map(|t| t.parse().ok())
                .collect(),
        })
    }

    fn to_item(&self) -> HashMap<String, AttributeValue> {
        let mut m = HashMap::new();
        m.insert("pk".into(), s(&relay::install_pk(&self.id)));
        m.insert("sk".into(), s("INSTALL"));
        m.insert("install_id".into(), s(&self.id));
        m.insert("status".into(), s(&self.status));
        m.insert("cloudfront".into(), s(&self.cloudfront));
        m.insert("verified_at".into(), s(&self.verified_at));
        m.insert("name".into(), s(&self.name));
        m.insert("created_at".into(), s(&self.created_at));
        m.insert("version".into(), s(&self.version));
        m.insert("sharing".into(), AttributeValue::Bool(self.sharing));
        m.insert(
            "share_payloads".into(),
            AttributeValue::Bool(self.share_payloads),
        );
        if !self.integrations.is_empty() {
            m.insert(
                "integrations".into(),
                AttributeValue::Ss(self.integrations.clone()),
            );
        }
        let changes: Vec<String> = self.name_changes.iter().map(|t| t.to_string()).collect();
        m.insert("name_changes".into(), s(&changes.join(",")));
        m
    }

    pub fn active(&self) -> bool {
        self.status == "active"
    }

    pub fn verified(&self) -> bool {
        self.active() && !self.verified_at.is_empty()
    }

    pub fn origin(&self) -> String {
        format!("https://{}", self.cloudfront)
    }

    /// The origins the install's dashboard answers on (for OAuth redirects).
    pub fn origins(&self) -> Vec<String> {
        let mut o = vec![self.origin()];
        if !self.name.is_empty() {
            o.push(format!("https://{}", names::hostname(&self.name)));
        }
        o
    }

    /// The JSON the install and admins see (no secrets: there are none here).
    pub fn view(&self) -> Value {
        json!({
            "install_id": self.id,
            "status": self.status,
            "cloudfront": self.cloudfront,
            "verified": self.verified(),
            "verified_at": self.verified_at,
            "name": if self.name.is_empty() { Value::Null } else { json!(self.name) },
            "hostname": if self.name.is_empty() { Value::Null } else { json!(names::hostname(&self.name)) },
            "created_at": self.created_at,
            "version": self.version,
            "sharing": { "enabled": self.sharing, "payloads": self.share_payloads },
            "integrations": self.integrations,
        })
    }
}

/// May one more name change happen now (at most [`names::CHANGES_PER_DAY`] a day)?
pub fn name_change_allowed(changes: &[i64], now: i64) -> bool {
    changes.iter().filter(|t| now - **t < 86_400).count() < names::CHANGES_PER_DAY as usize
}

pub(crate) async fn load_install(
    state: &AppState,
    id: &str,
) -> Result<Option<Install>, (StatusCode, Json<Value>)> {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&relay::install_pk(id)))
        .key("sk", s("INSTALL"))
        .send()
        .await
        .map_err(|e| {
            warn!(error = %e, "hybrid: install read failed");
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?;
    Ok(r.item().and_then(Install::from_item))
}

async fn save_install(state: &AppState, i: &Install) -> Result<(), (StatusCode, Json<Value>)> {
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(i.to_item()))
        .send()
        .await
        .map_err(|e| {
            warn!(error = %e, "hybrid: install write failed");
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?;
    // The admin list.
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s("HYBRIDINSTALLS"))
        .item("sk", s(&i.id))
        .item("status", s(&i.status))
        .item("name", s(&i.name))
        .item("cloudfront", s(&i.cloudfront))
        .item("created_at", s(&i.created_at))
        .send()
        .await;
    Ok(())
}

/// One line in the audit log (kept 400 days).
pub(crate) async fn audit(state: &AppState, install: &str, action: &str, detail: Value) {
    let id = ulid::Ulid::new().to_string();
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s("HYBRIDAUDIT"))
        .item("sk", s(&id))
        .item("install_id", s(install))
        .item("action", s(action))
        .item("detail", s(&detail.to_string()))
        .item("at", s(&now()))
        .item(
            "ttl",
            AttributeValue::N((epoch() + 400 * 86_400).to_string()),
        )
        .send()
        .await
        .map_err(|e| warn!(error = %e, action, "hybrid: audit write failed"));
    info!(install, action, "hybrid audit");
}

/// The install a signed request comes from, after checking the signature.
/// The signature covers the full path the install called (`/hybrid/...`).
/// These routes are nested under `/hybrid`, where a handler's own `Uri` has
/// the prefix taken off, so only [`OriginalUri`] is accepted here.
pub(crate) async fn signed(
    state: &AppState,
    headers: &HeaderMap,
    method: &Method,
    uri: &OriginalUri,
    body: &[u8],
) -> Result<Install, (StatusCode, Json<Value>)> {
    let master = master(state).ok_or_else(not_here)?;
    let h = |k: &str| headers.get(k).and_then(|v| v.to_str().ok());
    let id = h(sign::INSTALL_HEADER).unwrap_or_default();
    if !sign::is_install_id(id) {
        return Err(fail(
            StatusCode::UNAUTHORIZED,
            "signature_missing",
            "Sign the request with the install's id.",
        ));
    }
    let secret = sign::install_secret(master, id);
    sign::verify_request(
        &secret,
        h(sign::TIMESTAMP_HEADER),
        h(sign::SIGNATURE_HEADER),
        method.as_str(),
        uri.path(),
        body,
        epoch(),
    )
    .map_err(|p| fail(StatusCode::UNAUTHORIZED, p.code(), p.message()))?;
    let install = load_install(state, id).await?.ok_or_else(|| {
        fail(
            StatusCode::UNAUTHORIZED,
            "unknown_install",
            "This install isn't registered. Register it again.",
        )
    })?;
    if !install.active() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            broker::BrokerRefusal::Revoked.code(),
            broker::BrokerRefusal::Revoked.message(),
        ));
    }
    Ok(install)
}

pub(crate) fn body_json(body: &[u8]) -> Value {
    serde_json::from_slice(body).unwrap_or(Value::Null)
}

// ── Registration and ownership ───────────────────────────────────────────

/// POST /hybrid/installs `{cloudfront, version}`: register an install. Answers
/// its id, registration secret and relay key once; the install keeps them
/// in its own secret. Nothing works until the install proves it answers on
/// that CloudFront address ([`verify`]).
pub async fn register(State(state): State<Arc<AppState>>, ip: ClientIp, body: Bytes) -> Reply {
    let master = master(&state).ok_or_else(not_here)?.to_string();
    rate_limit::allow_shared(&state, "hybrid-register", &ip.0, 10, 3600)
        .await
        .map_err(|_| {
            fail(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "Too many registrations from here. Try again in an hour.",
            )
        })?;
    let b = body_json(&body);
    let cloudfront = b["cloudfront"]
        .as_str()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if !names::is_cloudfront_domain(&cloudfront) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "bad_cloudfront",
            "Send the install's CloudFront address, such as d1234abcd.cloudfront.net.",
        ));
    }
    // A CloudFront address whose install CoderHelm turned off can't come
    // back as a new registration: revoking would mean nothing otherwise.
    if cloudfront_revoked(&state, &cloudfront).await {
        return Err(fail(
            StatusCode::FORBIDDEN,
            broker::BrokerRefusal::Revoked.code(),
            broker::BrokerRefusal::Revoked.message(),
        ));
    }
    let id = sign::new_install_id();
    let install = Install {
        id: id.clone(),
        status: "active".into(),
        cloudfront: cloudfront.clone(),
        created_at: now(),
        version: b["version"]
            .as_str()
            .unwrap_or("")
            .chars()
            .take(40)
            .collect(),
        ..Default::default()
    };
    save_install(&state, &install).await?;
    audit(&state, &id, "register", json!({ "cloudfront": cloudfront })).await;
    Ok(Json(json!({
        "install_id": id,
        "secret": sign::install_secret(&master, &id),
        "relay_key": sign::relay_key(&master, &id),
    })))
}

/// The record that keeps a revoked install's CloudFront address from registering again.
pub fn revoked_cloudfront_key(cloudfront: &str) -> String {
    format!(
        "HYBRIDREVOKED#{}",
        cloudfront.trim().trim_end_matches('.').to_ascii_lowercase()
    )
}

async fn cloudfront_revoked(state: &AppState, cloudfront: &str) -> bool {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&revoked_cloudfront_key(cloudfront)))
        .key("sk", s("REVOKED"))
        .send()
        .await
        .map(|r| r.item().is_some())
        // Can't tell: refuse, the person tries again in a minute.
        .unwrap_or(true)
}

/// Asks the install's CloudFront address to answer a challenge with its
/// registration secret. True when it does.
async fn prove(master: &str, install: &Install) -> bool {
    let challenge = broker::new_handle("ch");
    let url = format!(
        "{}/webhooks/relay/proof?challenge={challenge}",
        install.origin()
    );
    // Never a redirect, never more than a small answer (relay::install_client).
    let Ok(r) = relay::install_client()
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
    let v: Value = serde_json::from_slice(&relay::read_capped(r).await).unwrap_or_default();
    v["install_id"].as_str() == Some(install.id.as_str())
        && sign::check_proof(
            &sign::install_secret(master, &install.id),
            &challenge,
            v["proof"].as_str().unwrap_or(""),
        )
}

/// POST /hybrid/installs/verify (signed): prove the CloudFront address.
pub async fn verify(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let mut install = signed(&state, &headers, &method, &uri, &body).await?;
    let master = master(&state).ok_or_else(not_here)?.to_string();
    if !prove(&master, &install).await {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "proof_failed",
            "CoderHelm couldn't reach your install at its CloudFront address, or it answered with other keys. Check that it's running and that it has the new keys (it restarts after registering; give it a minute), then try again.",
        ));
    }
    install.verified_at = now();
    save_install(&state, &install).await?;
    audit(
        &state,
        &install.id,
        "verify",
        json!({ "cloudfront": install.cloudfront }),
    )
    .await;
    Ok(Json(install.view()))
}

/// GET /hybrid/status (signed): the install as this side sees it, with its links.
pub async fn status(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    let links = github_links(&state, &install.id).await;
    let slug = crate::auth::github_app::app_slug(&state).await;
    let mut v = install.view();
    v["github"] = json!({ "links": links, "app_slug": slug });
    v["jira"] = json!({ "link": super::hybrid_jira::jira_link(&state, &install.id).await });
    v["offered"] = json!(offered_ids(&state));
    Ok(Json(v))
}

/// PUT /hybrid/settings (signed) `{integrations, sharing: {enabled, payloads}}`.
pub async fn settings(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let mut install = signed(&state, &headers, &method, &uri, &body).await?;
    let b = body_json(&body);
    if let Some(list) = b["integrations"].as_array() {
        let allowed = offered_ids(&state);
        install.integrations = list
            .iter()
            .filter_map(Value::as_str)
            .filter(|i| allowed.contains(i))
            .map(str::to_string)
            .collect();
    }
    if let Some(sh) = b.get("sharing").filter(|v| v.is_object()) {
        install.sharing = sh["enabled"].as_bool().unwrap_or(false);
        // Payloads only ever with sharing on, and only when asked for.
        install.share_payloads = install.sharing && sh["payloads"].as_bool().unwrap_or(false);
    }
    save_install(&state, &install).await?;
    audit(&state, &install.id, "settings", json!({ "integrations": install.integrations, "sharing": install.sharing, "payloads": install.share_payloads })).await;
    Ok(Json(install.view()))
}

/// DELETE /hybrid/installs (signed): uninstall. Releases the name, unlinks
/// every installation and revokes the OAuth sign-ins.
pub async fn unregister(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let mut install = signed(&state, &headers, &method, &uri, &body).await?;
    release_name(&state, &mut install).await?;
    for l in github_links(&state, &install.id).await {
        unlink(&state, &install.id, l.installation_id).await;
    }
    super::hybrid_jira::unlink_all(&state, &install.id).await;
    install.status = "removed".into();
    install.integrations.clear();
    save_install(&state, &install).await?;
    audit(&state, &install.id, "unregister", json!({})).await;
    Ok(Json(json!({ "ok": true })))
}

// ── Names ────────────────────────────────────────────────────────────────

/// GET /hybrid/names/:name: is it free? Public (the guide checks as people
/// type, from docs.coderhelm.com, so any origin may read it), limited per
/// address.
pub async fn name_available(
    State(state): State<Arc<AppState>>,
    ip: ClientIp,
    Path(raw): Path<String>,
) -> Response {
    public_reply(name_check(&state, &ip, &raw).await)
}

/// A public answer any origin may read, never with credentials
/// (middleware::cors keeps it that way for every origin but the dashboard's).
pub fn public_reply(r: Reply) -> Response {
    let cors = [(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")];
    match r {
        Ok(v) => (cors, v).into_response(),
        Err((code, v)) => (code, cors, v).into_response(),
    }
}

async fn name_check(state: &AppState, ip: &ClientIp, raw: &str) -> Reply {
    master(state).ok_or_else(not_here)?;
    rate_limit::allow(ip, "hybrid-names", names::CHECKS_PER_MINUTE as u64, 60).map_err(|_| {
        fail(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Checking too fast. Wait a moment.",
        )
    })?;
    let name = match names::validate_name(raw) {
        Ok(n) => n,
        Err(p) => {
            return Ok(Json(
                json!({ "name": raw, "available": false, "code": p.code(), "error": p.message() }),
            ))
        }
    };
    let owner = name_owner(state, &name).await?;
    let taken = owner.is_some();
    Ok(Json(json!({
        "name": name,
        "hostname": names::hostname(&name),
        "available": !taken,
        "code": if taken { names::NameProblem::Taken.code() } else { "ok" },
        "error": if taken { Value::String(names::NameProblem::Taken.message().into()) } else { Value::Null },
    })))
}

async fn name_owner(
    state: &AppState,
    name: &str,
) -> Result<Option<String>, (StatusCode, Json<Value>)> {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&format!("HYBRIDNAME#{name}")))
        .key("sk", s("NAME"))
        .send()
        .await
        .map_err(|_| {
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?;
    Ok(r.item()
        .and_then(|i| i.get("install_id"))
        .and_then(|v| v.as_s().ok())
        .cloned())
}

/// PUT /hybrid/names (signed) `{name}`: claim a name for this install. One
/// name per install; claiming another releases the old one. The name
/// answers with the hosted "being set up" page right away.
pub async fn claim_name(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let mut install = signed(&state, &headers, &method, &uri, &body).await?;
    if !install.verified() {
        return Err(fail(
            StatusCode::CONFLICT,
            "not_verified",
            "Verify the install first: CoderHelm checks it answers on its CloudFront address.",
        ));
    }
    let name = names::validate_name(body_json(&body)["name"].as_str().unwrap_or(""))
        .map_err(|p| fail(StatusCode::BAD_REQUEST, p.code(), p.message()))?;
    if install.name == name {
        return Ok(Json(install.view()));
    }
    let t = epoch();
    if !name_change_allowed(&install.name_changes, t) {
        return Err(fail(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "That's five name changes today. Try again tomorrow.",
        ));
    }
    let put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(&format!("HYBRIDNAME#{name}")))
        .item("sk", s("NAME"))
        .item("install_id", s(&install.id))
        .item("claimed_at", s(&now()))
        .item("pending", AttributeValue::Bool(true))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await;
    if let Err(e) = put {
        let taken = e
            .as_service_error()
            .is_some_and(|se| se.is_conditional_check_failed_exception());
        return Err(if taken {
            fail(
                StatusCode::CONFLICT,
                names::NameProblem::Taken.code(),
                names::NameProblem::Taken.message(),
            )
        } else {
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        });
    }
    // The setup page for the name. CREATE: a name with any record already
    // in the zone (made outside this API) is refused, never overwritten.
    if let Err(e) = apply_dns(&name, &names::plan_claim(&name)).await {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", s(&format!("HYBRIDNAME#{name}")))
            .key("sk", s("NAME"))
            .send()
            .await;
        warn!(error = %e, name, "hybrid: claim's DNS refused");
        return Err(
            if e.contains("already exists") || e.contains("InvalidChangeBatch") {
                fail(
                    StatusCode::CONFLICT,
                    names::NameProblem::Reserved.code(),
                    names::NameProblem::Reserved.message(),
                )
            } else {
                fail(
                    StatusCode::BAD_GATEWAY,
                    "dns_write_failed",
                    "CoderHelm couldn't set up the name just now. Try again in a minute.",
                )
            },
        );
    }
    release_name(&state, &mut install).await?;
    install.name = name.clone();
    install.name_changes.retain(|x| t - *x < 86_400);
    install.name_changes.push(t);
    save_install(&state, &install).await?;
    audit(&state, &install.id, "claim_name", json!({ "name": name })).await;
    Ok(Json(install.view()))
}

static R53: tokio::sync::OnceCell<aws_sdk_route53::Client> = tokio::sync::OnceCell::const_new();

async fn route53() -> &'static aws_sdk_route53::Client {
    R53.get_or_init(|| async { aws_sdk_route53::Client::new(&aws_config::load_from_env().await) })
        .await
}

fn zone_id() -> Option<String> {
    std::env::var("HYBRID_ZONE_ID")
        .ok()
        .filter(|z| !z.trim().is_empty())
}

/// CloudFront's own hosted zone id, the one every CloudFront alias uses.
const CLOUDFRONT_ALIAS_ZONE: &str = "Z2FDTNDATAQYW2";

/// Runs a planned batch in one Route 53 change (atomic). Refuses anything
/// outside the name, whatever the plan says.
async fn apply_dns(name: &str, plan: &[names::DnsChange]) -> Result<(), String> {
    use aws_sdk_route53::types::{
        AliasTarget, Change, ChangeAction, ChangeBatch, ResourceRecord, ResourceRecordSet, RrType,
    };
    if plan.is_empty() {
        return Ok(());
    }
    if !names::changes_stay_inside(name, plan) {
        return Err("a change outside the name was refused".into());
    }
    let zone = zone_id().ok_or("The hosted zone isn't configured (HYBRID_ZONE_ID).")?;
    let fallback = std::env::var("HYBRID_FALLBACK_DOMAIN").unwrap_or_default();
    let mut changes = Vec::new();
    for c in plan {
        let (action, r) = match c {
            names::DnsChange::Create(r) => (ChangeAction::Create, r),
            names::DnsChange::Upsert(r) => (ChangeAction::Upsert, r),
            names::DnsChange::Delete(r) => (ChangeAction::Delete, r),
        };
        let set = match r {
            names::DnsRecord::Cname { name, value } => ResourceRecordSet::builder()
                .name(name)
                .r#type(RrType::Cname)
                .ttl(300)
                .resource_records(
                    ResourceRecord::builder()
                        .value(value)
                        .build()
                        .map_err(|e| e.to_string())?,
                )
                .build(),
            names::DnsRecord::FallbackAlias { name, ipv6 } => {
                if !names::is_cloudfront_domain(&fallback) {
                    return Err(
                        "The fallback site isn't configured (HYBRID_FALLBACK_DOMAIN).".into(),
                    );
                }
                ResourceRecordSet::builder()
                    .name(name)
                    .r#type(if *ipv6 { RrType::Aaaa } else { RrType::A })
                    .alias_target(
                        AliasTarget::builder()
                            .hosted_zone_id(CLOUDFRONT_ALIAS_ZONE)
                            .dns_name(&fallback)
                            .evaluate_target_health(false)
                            .build()
                            .map_err(|e| e.to_string())?,
                    )
                    .build()
            }
        }
        .map_err(|e| e.to_string())?;
        changes.push(
            Change::builder()
                .action(action)
                .resource_record_set(set)
                .build()
                .map_err(|e| e.to_string())?,
        );
    }
    route53()
        .await
        .change_resource_record_sets()
        .hosted_zone_id(zone)
        .change_batch(
            ChangeBatch::builder()
                .set_changes(Some(changes))
                .comment("coderhelm hybrid")
                .build()
                .map_err(|e| e.to_string())?,
        )
        .send()
        .await
        .map_err(|e| aws_sdk_route53::error::DisplayErrorContext(&e).to_string())?;
    Ok(())
}

/// What this API wrote for a name: its records and whether the setup-page
/// alias is still there.
async fn stored_name(state: &AppState, name: &str) -> (Vec<names::NameRecord>, bool) {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&format!("HYBRIDNAME#{name}")))
        .key("sk", s("NAME"))
        .send()
        .await
        .ok();
    let item = r.and_then(|r| r.item().cloned()).unwrap_or_default();
    let recs = item
        .get("records")
        .and_then(|v| v.as_s().ok())
        .and_then(|t| serde_json::from_str(t).ok())
        .unwrap_or_default();
    let pending = item
        .get("pending")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    (recs, pending)
}

/// PUT /hybrid/names/records (signed) `{records: [...]}`: write the
/// certificate's validation record and, once the certificate is issued, the
/// address record for this install's name. Each record is checked; the
/// address must point at this install's own, verified CloudFront
/// distribution (proved again now). New records are created, never written
/// over; only records this API wrote before are ever changed.
pub async fn put_records(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    if install.name.is_empty() {
        return Err(fail(StatusCode::CONFLICT, "no_name", "Claim a name first."));
    }
    let master = master(&state).ok_or_else(not_here)?.to_string();
    let asked: Vec<names::NameRecord> = serde_json::from_value(body_json(&body)["records"].clone())
        .map_err(|_| {
            fail(
                StatusCode::BAD_REQUEST,
                "bad_records",
                "Send records as a list of {purpose, name, type, value}.",
            )
        })?;
    if asked.is_empty() || asked.len() > 2 {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "bad_records",
            "Send the certificate record, the address record, or both.",
        ));
    }
    let mut ok = Vec::new();
    for r in &asked {
        ok.push(
            names::check_record(&install.name, r, &install.cloudfront)
                .map_err(|why| fail(StatusCode::BAD_REQUEST, "record_refused", why))?,
        );
    }
    if !prove(&master, &install).await {
        return Err(fail(StatusCode::BAD_REQUEST, "proof_failed", "CoderHelm couldn't confirm your install at its CloudFront address just now. Try again in a minute."));
    }
    let (old, pending) = stored_name(&state, &install.name).await;
    let plan = names::plan_records(&install.name, &old, pending, &ok);
    apply_dns(&install.name, &plan).await.map_err(|e| {
        warn!(error = %e, install = %install.id, "hybrid: Route 53 write failed");
        if e.contains("already exists") {
            fail(
                StatusCode::CONFLICT,
                "record_exists",
                "A record CoderHelm didn't make is already there for this name. Contact support.",
            )
        } else {
            fail(
                StatusCode::BAD_GATEWAY,
                "dns_write_failed",
                "CoderHelm couldn't write the records just now. Try again in a minute.",
            )
        }
    })?;
    let mut keep: Vec<names::NameRecord> = old
        .into_iter()
        .filter(|o| !ok.iter().any(|n| n.purpose == o.purpose))
        .collect();
    keep.extend(ok.clone());
    let still_pending = pending && !ok.iter().any(|r| r.purpose == "address");
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&format!("HYBRIDNAME#{}", install.name)))
        .key("sk", s("NAME"))
        .update_expression("SET records = :r, records_at = :t, pending = :p")
        .expression_attribute_values(":r", s(&serde_json::to_string(&keep).unwrap_or_default()))
        .expression_attribute_values(":t", s(&now()))
        .expression_attribute_values(":p", AttributeValue::Bool(still_pending))
        .send()
        .await;
    audit(
        &state,
        &install.id,
        "put_records",
        json!({ "name": install.name, "records": ok }),
    )
    .await;
    Ok(Json(
        json!({ "ok": true, "records": keep, "pending": still_pending }),
    ))
}

/// Removes the install's name and its records (claiming another, uninstall, revoke).
async fn release_name(
    state: &AppState,
    install: &mut Install,
) -> Result<(), (StatusCode, Json<Value>)> {
    if install.name.is_empty() {
        return Ok(());
    }
    let name = install.name.clone();
    let (recs, pending) = stored_name(state, &name).await;
    if let Err(e) = apply_dns(&name, &names::plan_release(&name, &recs, pending)).await {
        warn!(error = %e, name, "hybrid: records not removed on release");
    }
    let _ = state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&format!("HYBRIDNAME#{name}")))
        .key("sk", s("NAME"))
        .condition_expression("install_id = :i")
        .expression_attribute_values(":i", s(&install.id))
        .send()
        .await;
    install.name.clear();
    install.name_changes.push(epoch());
    audit(state, &install.id, "release_name", json!({ "name": name })).await;
    Ok(())
}

/// DELETE /hybrid/names (signed): release this install's name.
pub async fn delete_name(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let mut install = signed(&state, &headers, &method, &uri, &body).await?;
    release_name(&state, &mut install).await?;
    save_install(&state, &install).await?;
    Ok(Json(install.view()))
}

// ── The fallback page for every {name}.coderhelm.com ─────────────────────

/// Which page an address gets: nobody's (404, the same page for reserved,
/// released or revoked names: it never says why), or still being set up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackPage {
    Nobody,
    SettingUp,
}

/// The name an address asks for, when it is exactly one label under the zone.
pub fn name_from_host(host: &str) -> Option<String> {
    let h = host
        .trim()
        .split(':')
        .next()
        .unwrap_or("")
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let label = h.strip_suffix(&format!(".{}", names::zone()))?;
    (!label.is_empty() && !label.contains('.')).then(|| label.to_string())
}

/// The page, status and headers for a fallback request (pure; tested).
pub fn fallback_response(
    page: FallbackPage,
    path: &str,
) -> (StatusCode, Vec<(&'static str, &'static str)>, String) {
    let mut headers = vec![
        ("x-robots-tag", "noindex, nofollow"),
        ("cache-control", "public, max-age=60"),
    ];
    if path == "/robots.txt" {
        headers.push(("content-type", "text/plain; charset=utf-8"));
        return (
            StatusCode::OK,
            headers,
            "User-agent: *\nDisallow: /\n".to_string(),
        );
    }
    headers.push(("content-type", "text/html; charset=utf-8"));
    let (status, title, lead, extra) = match page {
        FallbackPage::Nobody => (
            StatusCode::NOT_FOUND,
            "There\u{2019}s no CoderHelm here yet",
            "Nobody runs CoderHelm at this address. Check the address for a typo, or set up your own.",
            r#"<a class="primary" href="https://docs.coderhelm.com/self-host/install/">Set up your own CoderHelm</a><a href="https://app.coderhelm.com/">Sign in to coderhelm.com</a>"#,
        ),
        FallbackPage::SettingUp => {
            headers.push(("retry-after", "300"));
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "This CoderHelm is still being set up",
                "Its address was just claimed. AWS is issuing the certificate and the install moves here by itself, usually within 30 minutes. This page goes away when it\u{2019}s ready.",
                r#"<a class="primary" href="">Check again</a><a href="https://docs.coderhelm.com/self-host/hybrid/">How CoderHelm addresses work</a>"#,
            )
        }
    };
    let html = format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>{title}</title><style>
:root{{--bg:#fafafa;--fg:#111;--muted:#5c5c66;--line:#e4e4e7;--brand:#2563eb;--on-brand:#fff;--card:#fff}}
@media (prefers-color-scheme:dark){{:root{{--bg:#0a0a0b;--fg:#f4f4f5;--muted:#a1a1aa;--line:#27272a;--brand:#60a5fa;--on-brand:#0b1220;--card:#111113}}}}
*{{box-sizing:border-box}}body{{margin:0;min-height:100vh;display:grid;place-items:center;background:var(--bg);color:var(--fg);font:16px/1.55 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;padding:24px 16px}}
main{{max-width:520px;width:100%;background:var(--card);border:1px solid var(--line);border-radius:16px;padding:32px}}
.mark{{display:flex;align-items:center;gap:8px;font-weight:700;letter-spacing:-.01em;margin-bottom:28px}}.mark svg{{color:var(--brand)}}
h1{{font-size:24px;line-height:1.25;margin:0 0 10px;letter-spacing:-.02em}}p{{margin:0 0 24px;color:var(--muted)}}
.actions{{display:flex;flex-wrap:wrap;gap:10px}}a{{display:inline-flex;align-items:center;min-height:44px;padding:0 16px;border-radius:10px;border:1px solid var(--line);color:var(--fg);text-decoration:none;font-weight:600}}
a.primary{{background:var(--brand);border-color:var(--brand);color:var(--on-brand)}}@media (max-width:420px){{main{{padding:24px}}.actions a{{width:100%;justify-content:center}}}}
</style></head><body><main><div class="mark"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M7 4L3 20M13 4L9 20M19 4L15 20"/></svg>CoderHelm</div><h1>{title}</h1><p>{lead}</p><div class="actions">{extra}</div></main></body></html>"#
    );
    (status, headers, html)
}

/// GET /hybrid/fallback: the page every `{name}.coderhelm.com` without its
/// own record shows (the wildcard record's site sends the viewer's host in
/// `x-coderhelm-host`).
pub async fn fallback(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let host = headers
        .get("x-coderhelm-host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let path = headers
        .get("x-coderhelm-path")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(uri.path())
        .to_string();
    let mut page = FallbackPage::Nobody;
    if let Some(name) = name_from_host(host).filter(|n| names::validate_name(n).is_ok()) {
        if let Ok(Some(owner)) = name_owner(&state, &name).await {
            if let Ok(Some(i)) = load_install(&state, &owner).await {
                if i.active() && i.name == name {
                    page = FallbackPage::SettingUp;
                }
            }
        }
    }
    let (status, hs, body) = fallback_response(page, &path);
    let mut r = (status, body).into_response();
    for (k, v) in hs {
        if let Ok(v) = axum::http::HeaderValue::from_str(v) {
            r.headers_mut().insert(k, v);
        }
    }
    r
}

// ── GitHub: links, relay, broker ─────────────────────────────────────────

/// The GitHub installations linked to an install.
pub async fn github_links(state: &AppState, install: &str) -> Vec<broker::GithubLink> {
    let r = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :p AND begins_with(sk, :g)")
        .expression_attribute_values(":p", s(&relay::install_pk(install)))
        .expression_attribute_values(":g", s("GHLINK#"))
        .send()
        .await;
    r.ok()
        .map(|r| {
            r.items()
                .iter()
                .filter_map(|i| i.get("data").and_then(|v| v.as_s().ok()))
                .filter_map(|d| serde_json::from_str(d).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// The install a GitHub installation is linked to, if any (the relay's route).
pub async fn install_for_installation(state: &AppState, installation_id: u64) -> Option<String> {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&format!("HYBRIDGH#{installation_id}")))
        .key("sk", s("LINK"))
        .send()
        .await
        .ok()?;
    r.item()?.get("install_id")?.as_s().ok().cloned()
}

async fn unlink(state: &AppState, install: &str, installation_id: u64) {
    for (pk, sk) in [
        (
            relay::install_pk(install),
            format!("GHLINK#{installation_id}"),
        ),
        (format!("HYBRIDGH#{installation_id}"), "LINK".to_string()),
    ] {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", s(&pk))
            .key("sk", s(&sk))
            .send()
            .await;
    }
    audit(
        state,
        install,
        "unlink_github",
        json!({ "installation_id": installation_id }),
    )
    .await;
}

/// POST /hybrid/github/link-start (signed) `{return_url}`: where to send the
/// person to install CoderHelm's GitHub App on their organization. The
/// state ties GitHub's answer back to this install for one hour.
pub async fn link_start(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    if !install.verified() {
        return Err(fail(
            StatusCode::CONFLICT,
            "not_verified",
            "Verify the install first.",
        ));
    }
    let ret = return_url_for(
        &install,
        body_json(&body)["return_url"].as_str().unwrap_or(""),
    );
    let st = broker::new_handle("hyb");
    put_state(
        &state,
        &format!("HYBRIDSTATE#{st}"),
        &[("install_id", &install.id), ("return_url", &ret)],
        3600,
    )
    .await?;
    let slug = crate::auth::github_app::app_slug(&state)
        .await
        .ok_or_else(|| {
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "github_unavailable",
                "CoderHelm couldn't reach GitHub. Try again in a minute.",
            )
        })?;
    Ok(Json(json!({
        "state": st,
        "install_url": format!("https://github.com/apps/{slug}/installations/new?state={st}"),
    })))
}

/// Where the person goes back to after linking: an address on one of the
/// install's own hosts (or the docs install guide), compared host by host,
/// never by text prefix (`https://d1.cloudfront.net.example.org` starts with
/// the install's origin too); anything else gets the install's settings page.
pub fn return_url_for(install: &Install, asked: &str) -> String {
    let fallback = format!("{}/settings/system/#hybrid", install.origin());
    let Ok(u) = url::Url::parse(asked.trim()) else {
        return fallback;
    };
    if u.scheme() != "https"
        || !u.username().is_empty()
        || u.password().is_some()
        || u.port().is_some()
    {
        return fallback;
    }
    let host = u.host_str().unwrap_or("").to_ascii_lowercase();
    let mut hosts = vec![install.cloudfront.clone(), "docs.coderhelm.com".to_string()];
    if !install.name.is_empty() {
        hosts.push(names::hostname(&install.name));
    }
    if hosts.contains(&host) {
        u.to_string()
    } else {
        fallback
    }
}

pub(crate) async fn put_state(
    state: &AppState,
    pk: &str,
    fields: &[(&str, &str)],
    ttl_secs: i64,
) -> Result<(), (StatusCode, Json<Value>)> {
    let mut req = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(pk))
        .item("sk", s("STATE"))
        .item("ttl", AttributeValue::N((epoch() + ttl_secs).to_string()))
        .item(
            "expires",
            AttributeValue::N((epoch() + ttl_secs).to_string()),
        );
    for (k, v) in fields {
        req = req.item(*k, s(v));
    }
    req.send().await.map_err(|_| {
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "store_unavailable",
            "Try again in a minute.",
        )
    })?;
    Ok(())
}

/// Takes a one-time state (deleted as it is read). None when unknown or expired.
pub(crate) async fn take_state(
    state: &AppState,
    pk: &str,
) -> Option<HashMap<String, AttributeValue>> {
    let r = state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(pk))
        .key("sk", s("STATE"))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
        .send()
        .await
        .ok()?;
    let item = r.attributes()?.clone();
    let expires: i64 = item
        .get("expires")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    (expires >= epoch()).then_some(item)
}

#[derive(Deserialize, Default)]
pub struct SetupQuery {
    installation_id: Option<u64>,
    state: Option<String>,
    code: Option<String>,
    setup_action: Option<String>,
}

fn page_redirect(base: &str, outcome: &str) -> Response {
    let (path, frag) = base
        .split_once('#')
        .map(|(p, f)| (p.to_string(), format!("#{f}")))
        .unwrap_or((base.to_string(), String::new()));
    let sep = if path.contains('?') { '&' } else { '?' };
    Redirect::to(&format!("{path}{sep}hybrid_github={outcome}{frag}")).into_response()
}

/// GET /hybrid/github/setup?installation_id=&state=: where the dashboard's
/// GitHub setup page sends a hybrid install's return from GitHub. To prove
/// the person controls the installation (GitHub's redirect alone can be
/// typed by anyone), it first signs them in with GitHub and checks that
/// installation is theirs; then it links it to the install that started.
pub async fn github_setup(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SetupQuery>,
) -> Response {
    if master(&state).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let (Some(iid), Some(st)) = (q.installation_id, q.state.clone()) else {
        return (StatusCode::BAD_REQUEST, "Missing installation_id or state.").into_response();
    };
    if !st.starts_with("hyb_") || st.len() > 60 {
        return (StatusCode::BAD_REQUEST, "Unknown state.").into_response();
    }
    let _ = q.setup_action;
    let Some(code) = q.code.clone().filter(|c| !c.is_empty()) else {
        // Sign in with GitHub first; GitHub comes back to the callback below.
        let pending = format!("{st}.{iid}");
        let redirect = format!(
            "{}/auth/github/callback/hybrid",
            common::public_urls::api_url(&state.config.stage)
        );
        let url = format!(
            "https://github.com/login/oauth/authorize?client_id={}&state={}&redirect_uri={}",
            urlencoding::encode(&state.secrets.github_client_id),
            urlencoding::encode(&pending),
            urlencoding::encode(&redirect)
        );
        return Redirect::to(&url).into_response();
    };
    link_with_code(&state, &st, iid, &code).await
}

#[derive(Deserialize, Default)]
pub struct OAuthReturn {
    code: Option<String>,
    state: Option<String>,
}

/// GET /auth/github/callback/hybrid?code=&state=<state>.<installation>: the
/// GitHub sign-in that proves the installation is the person's.
pub async fn github_signin_return(
    State(state): State<Arc<AppState>>,
    Query(q): Query<OAuthReturn>,
) -> Response {
    if master(&state).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let (Some(code), Some(pending)) = (q.code, q.state) else {
        return (StatusCode::BAD_REQUEST, "Missing code or state.").into_response();
    };
    let Some((st, iid)) = pending
        .split_once('.')
        .and_then(|(s, i)| Some((s.to_string(), i.parse::<u64>().ok()?)))
    else {
        return (StatusCode::BAD_REQUEST, "Unknown state.").into_response();
    };
    link_with_code(&state, &st, iid, &code).await
}

/// Does this GitHub user see the installation of CoderHelm's own app
/// (`GET /user/installations`)? An installation of any other app never counts.
pub fn user_sees_installation(
    user_installations: &Value,
    installation_id: u64,
    app_id: u64,
) -> Option<String> {
    if app_id == 0 {
        return None;
    }
    user_installations["installations"]
        .as_array()?
        .iter()
        .find(|i| i["id"].as_u64() == Some(installation_id) && i["app_id"].as_u64() == Some(app_id))
        .map(|i| i["account"]["login"].as_str().unwrap_or("").to_string())
}

/// The repositories in the installation the person may push to (their own
/// permissions, asked with their token), at most [`broker::MAX_CEILING`].
async fn writable_in_installation(
    state: &AppState,
    user_token: &str,
    installation_id: u64,
) -> Result<Vec<String>, ()> {
    let mut out: Vec<String> = Vec::new();
    for page in 1..=10 {
        let v: Value = state
            .http
            .get(format!(
                "https://api.github.com/user/installations/{installation_id}/repositories?per_page=100&page={page}"
            ))
            .header("Authorization", format!("Bearer {user_token}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .send()
            .await
            .map_err(|_| ())?
            .json()
            .await
            .map_err(|_| ())?;
        let n = v["repositories"].as_array().map_or(0, Vec::len);
        for r in broker::writable_repositories(&v) {
            if !out.contains(&r) && out.len() < broker::MAX_CEILING {
                out.push(r);
            }
        }
        if n < 100 || out.len() >= broker::MAX_CEILING {
            break;
        }
    }
    Ok(out)
}

/// The page that asks the person to confirm the link: which GitHub account,
/// which install (by its own address), and how many repositories. Nothing
/// is linked until they press the button; it can't be framed.
pub fn confirm_page(account: &str, install_address: &str, repos: usize, token: &str) -> Response {
    let esc = |t: &str| {
        t.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&#39;")
    };
    let (account, address, token) = (esc(account), esc(install_address), esc(token));
    let plural = if repos == 1 {
        "repository"
    } else {
        "repositories"
    };
    let html = format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>Link GitHub to a self-hosted CoderHelm</title><style>
:root{{--bg:#fafafa;--fg:#111;--muted:#5c5c66;--line:#e4e4e7;--brand:#2563eb;--on-brand:#fff;--card:#fff;--warn:#92400e}}
@media (prefers-color-scheme:dark){{:root{{--bg:#0a0a0b;--fg:#f4f4f5;--muted:#a1a1aa;--line:#27272a;--brand:#60a5fa;--on-brand:#0b1220;--card:#111113;--warn:#fbbf24}}}}
*{{box-sizing:border-box}}body{{margin:0;min-height:100vh;display:grid;place-items:center;background:var(--bg);color:var(--fg);font:16px/1.55 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;padding:24px 16px}}
main{{max-width:560px;width:100%;background:var(--card);border:1px solid var(--line);border-radius:16px;padding:32px}}
h1{{font-size:22px;line-height:1.3;margin:0 0 12px}}p{{margin:0 0 16px;color:var(--muted)}}code{{color:var(--fg);font-weight:600;word-break:break-all}}.warn{{color:var(--warn)}}
form{{display:flex;flex-wrap:wrap;gap:10px;margin-top:24px}}button{{min-height:44px;padding:0 16px;border-radius:10px;border:1px solid var(--line);background:transparent;color:var(--fg);font:inherit;font-weight:600;cursor:pointer}}
button.primary{{background:var(--brand);border-color:var(--brand);color:var(--on-brand)}}@media (max-width:420px){{main{{padding:24px}}button{{width:100%}}}}
</style></head><body><main><h1>Link <code>{account}</code> to a self-hosted CoderHelm?</h1>
<p>The CoderHelm install at <code>{address}</code> will get short-lived GitHub tokens that can read and change code in the {repos} {plural} you can push to in <code>{account}</code>, and it will receive that account's GitHub events.</p>
<p class="warn">Only continue if you started this from that install yourself. If someone sent you this link, choose Cancel.</p>
<form method="post" action="/hybrid/github/confirm"><input type="hidden" name="token" value="{token}"><button class="primary" type="submit" name="choice" value="link">Link {account}</button><button type="submit" name="choice" value="cancel">Cancel</button></form></main></body></html>"#
    );
    let mut r = (StatusCode::OK, html).into_response();
    for (k, v) in [
        ("content-type", "text/html; charset=utf-8"),
        ("cache-control", "no-store"),
        ("x-frame-options", "DENY"),
        (
            "content-security-policy",
            "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'",
        ),
        ("referrer-policy", "no-referrer"),
        ("x-robots-tag", "noindex, nofollow"),
    ] {
        if let Ok(v) = axum::http::HeaderValue::from_str(v) {
            r.headers_mut().insert(k, v);
        }
    }
    r
}

async fn link_with_code(state: &AppState, st: &str, iid: u64, code: &str) -> Response {
    let Some(pending) = take_state(state, &format!("HYBRIDSTATE#{st}")).await else {
        return (
            StatusCode::BAD_REQUEST,
            "That link expired or was already used. Start it again from your install.",
        )
            .into_response();
    };
    let g = |k: &str| {
        pending
            .get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let (install_id, ret) = (g("install_id"), g("return_url"));
    // Trade the code for the person's token, then look at their installations.
    let tok: Value = match state
        .http
        .post("https://github.com/login/oauth/access_token")
        .header("Accept", "application/json")
        .json(&json!({ "client_id": state.secrets.github_client_id, "client_secret": state.secrets.github_client_secret, "code": code }))
        .send()
        .await
    {
        Ok(r) => r.json().await.unwrap_or_default(),
        Err(_) => return page_redirect(&ret, "github_unreachable"),
    };
    let Some(user_token) = tok["access_token"].as_str() else {
        return page_redirect(&ret, "signin_failed");
    };
    let installs: Value = match state
        .http
        .get("https://api.github.com/user/installations?per_page=100")
        .header("Authorization", format!("Bearer {user_token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .send()
        .await
    {
        Ok(r) => r.json().await.unwrap_or_default(),
        Err(_) => return page_redirect(&ret, "github_unreachable"),
    };
    let app_id: u64 = state.secrets.github_app_id.trim().parse().unwrap_or(0);
    let Some(account) = user_sees_installation(&installs, iid, app_id) else {
        audit(
            state,
            &install_id,
            "link_refused",
            json!({ "installation_id": iid, "why": "not the person's installation" }),
        )
        .await;
        return page_redirect(&ret, "not_yours");
    };
    if let Some(r) = link_blocked(state, &install_id, iid).await {
        return page_redirect(&ret, r);
    }
    // What the person may push to themselves: the link never reaches past it.
    let Ok(ceiling) = writable_in_installation(state, user_token, iid).await else {
        return page_redirect(&ret, "github_unreachable");
    };
    if ceiling.is_empty() {
        audit(
            state,
            &install_id,
            "link_refused",
            json!({ "installation_id": iid, "why": "no repository the person can push to" }),
        )
        .await;
        return page_redirect(&ret, "no_write_access");
    }
    let Ok(Some(install)) = load_install(state, &install_id).await else {
        return page_redirect(&ret, "install_off");
    };
    let token = broker::new_handle("hl");
    let ceiling_json = serde_json::to_string(&ceiling).unwrap_or_default();
    let iid_s = iid.to_string();
    if put_state(
        state,
        &format!("HYBRIDCONFIRM#{token}"),
        &[
            ("install_id", &install_id),
            ("installation_id", &iid_s),
            ("account", &account),
            ("return_url", &ret),
            ("ceiling", &ceiling_json),
        ],
        600,
    )
    .await
    .is_err()
    {
        return page_redirect(&ret, "store_failed");
    }
    let address = if install.name.is_empty() {
        install.cloudfront.clone()
    } else {
        names::hostname(&install.name)
    };
    confirm_page(&account, &address, ceiling.len(), &token)
}

/// Why an installation can't be linked to this install right now, if it can't.
async fn link_blocked(state: &AppState, install_id: &str, iid: u64) -> Option<&'static str> {
    match load_install(state, install_id).await {
        Ok(Some(i)) if i.verified() => {}
        _ => return Some("install_off"),
    }
    // Already linked elsewhere: one install per installation.
    if let Some(other) = install_for_installation(state, iid).await {
        if other != install_id {
            return Some("linked_elsewhere");
        }
    }
    // A hosted team already owns it: never take a hosted customer's installation.
    if super::github_webhook::resolve_team_by_installation(state, iid)
        .await
        .is_some()
    {
        return Some("used_by_hosted_team");
    }
    None
}

/// POST /hybrid/github/confirm `token=&choice=link|cancel` (form, from
/// [`confirm_page`]): the person's answer. Only `link` links.
pub async fn github_confirm(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    if master(&state).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let f = form(&body);
    let token = f.get("token").cloned().unwrap_or_default();
    if !token.starts_with("hl_") || token.len() > 60 {
        return (StatusCode::BAD_REQUEST, "Unknown link.").into_response();
    }
    let Some(p) = take_state(&state, &format!("HYBRIDCONFIRM#{token}")).await else {
        return (
            StatusCode::BAD_REQUEST,
            "That link expired or was already used. Start it again from your install.",
        )
            .into_response();
    };
    let g = |k: &str| {
        p.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let (install_id, ret, account) = (g("install_id"), g("return_url"), g("account"));
    let Ok(iid) = g("installation_id").parse::<u64>() else {
        return (StatusCode::BAD_REQUEST, "Unknown link.").into_response();
    };
    if f.get("choice").map(String::as_str) != Some("link") {
        audit(
            &state,
            &install_id,
            "link_declined",
            json!({ "installation_id": iid }),
        )
        .await;
        return page_redirect(&ret, "declined");
    }
    if let Some(r) = link_blocked(&state, &install_id, iid).await {
        return page_redirect(&ret, r);
    }
    let ceiling: Vec<String> = serde_json::from_str(&g("ceiling")).unwrap_or_default();
    if ceiling.is_empty() {
        return page_redirect(&ret, "no_write_access");
    }
    let link = broker::GithubLink {
        installation_id: iid,
        account: account.clone(),
        repositories: vec![],
        ceiling: Some(ceiling.clone()),
    };
    let ok = match link_writes(&state.config.settings_table_name, &install_id, &link) {
        Ok(items) => state
            .dynamo
            .transact_write_items()
            .set_transact_items(Some(items))
            .send()
            .await
            .is_ok(),
        Err(_) => false,
    };
    if !ok {
        return page_redirect(&ret, "store_failed");
    }
    audit(
        &state,
        &install_id,
        "link_github",
        json!({ "installation_id": iid, "account": account, "repositories": ceiling.len() }),
    )
    .await;
    page_redirect(&ret, "linked")
}

/// The link under the install and the installation's reverse index, written
/// together or not at all: the reverse index is never written over another
/// install's link made in the meantime, and then the link isn't kept either.
fn link_writes(
    table: &str,
    install_id: &str,
    link: &broker::GithubLink,
) -> Result<Vec<TransactWriteItem>, aws_sdk_dynamodb::error::BuildError> {
    let iid = link.installation_id;
    let own = Put::builder()
        .table_name(table)
        .item("pk", s(&relay::install_pk(install_id)))
        .item("sk", s(&format!("GHLINK#{iid}")))
        .item("data", s(&serde_json::to_string(link).unwrap_or_default()))
        .item("linked_at", s(&now()))
        .build()?;
    let reverse = Put::builder()
        .table_name(table)
        .item("pk", s(&format!("HYBRIDGH#{iid}")))
        .item("sk", s("LINK"))
        .item("install_id", s(install_id))
        .condition_expression("attribute_not_exists(pk) OR install_id = :i")
        .expression_attribute_values(":i", s(install_id))
        .build()?;
    Ok(vec![
        TransactWriteItem::builder().put(own).build(),
        TransactWriteItem::builder().put(reverse).build(),
    ])
}

/// PUT /hybrid/github/links/:installation_id (signed) `{repositories}`: the
/// repositories this install may get tokens for (empty: every repository in
/// the link's ceiling).
pub async fn set_link_repos(
    State(state): State<Arc<AppState>>,
    Path(iid): Path<u64>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    let mut links = github_links(&state, &install.id).await;
    let link = links
        .iter_mut()
        .find(|l| l.installation_id == iid)
        .ok_or_else(|| {
            fail(
                StatusCode::NOT_FOUND,
                broker::BrokerRefusal::NotLinked.code(),
                broker::BrokerRefusal::NotLinked.message(),
            )
        })?;
    let chosen: Vec<String> = body_json(&body)["repositories"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(|r| r.trim().to_string())
                .filter(|r| !r.is_empty())
                .take(1000)
                .collect()
        })
        .unwrap_or_default();
    if link.ceiling.is_none() {
        let r = broker::BrokerRefusal::RelinkNeeded;
        return Err(fail(StatusCode::FORBIDDEN, r.code(), r.message()));
    }
    if !broker::ceiling_allows(link, &chosen) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            broker::BrokerRefusal::RepositoryNotAllowed.code(),
            broker::BrokerRefusal::RepositoryNotAllowed.message(),
        ));
    }
    link.repositories = chosen;
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&relay::install_pk(&install.id)))
        .key("sk", s(&format!("GHLINK#{iid}")))
        .update_expression("SET #d = :d")
        .expression_attribute_names("#d", "data")
        .expression_attribute_values(":d", s(&serde_json::to_string(&*link).unwrap_or_default()))
        .send()
        .await
        .map_err(|_| {
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?;
    audit(
        &state,
        &install.id,
        "set_repositories",
        json!({ "installation_id": iid, "count": link.repositories.len() }),
    )
    .await;
    Ok(Json(json!({ "link": link })))
}

/// DELETE /hybrid/github/links/:installation_id (signed).
pub async fn delete_link(
    State(state): State<Arc<AppState>>,
    Path(iid): Path<u64>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    unlink(&state, &install.id, iid).await;
    Ok(Json(json!({ "ok": true })))
}

/// POST /hybrid/github/token (signed) `{installation_id, repositories?, permissions?}`:
/// a short-lived installation token, never wider than the link.
pub async fn github_token(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    let ask: broker::TokenAsk = serde_json::from_slice(&body).map_err(|_| {
        fail(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "Send {installation_id}.",
        )
    })?;
    let mut d = Delivery::new(Direction::Outbound, "broker", "github.token", &install.id);
    d.external_id = ask.installation_id.to_string();
    let started = std::time::Instant::now();
    let links = github_links(&state, &install.id).await;
    let plan = match broker::plan_token(&links, &ask) {
        Ok(p) => p,
        Err(r) => {
            d.finish(Status::Failed, "Refused", r.code());
            deliveries::record(
                &state.dynamo,
                &state.config.settings_table_name,
                &relay::install_pk(&install.id),
                &d,
            )
            .await;
            return Err(fail(StatusCode::FORBIDDEN, r.code(), r.message()));
        }
    };
    match crate::auth::github_app::mint_scoped(&state, ask.installation_id, &plan).await {
        Ok((token, expires_at)) => {
            d.push_attempt(deliveries::Attempt {
                at: now(),
                code: Some(201),
                duration_ms: started.elapsed().as_millis() as u64,
                error: String::new(),
            });
            d.finish(Status::Handled, "Token minted", "");
            deliveries::record(
                &state.dynamo,
                &state.config.settings_table_name,
                &relay::install_pk(&install.id),
                &d,
            )
            .await;
            let slug = crate::auth::github_app::app_slug(&state)
                .await
                .unwrap_or_else(|| "coderhelm".into());
            Ok(Json(json!({
                "token": token,
                "expires_at": expires_at,
                "repositories": plan.repositories,
                "permissions": plan.permissions,
                "bot_login": format!("{slug}[bot]"),
            })))
        }
        Err((code, msg)) => {
            d.push_attempt(deliveries::Attempt {
                at: now(),
                code,
                duration_ms: started.elapsed().as_millis() as u64,
                error: msg.clone(),
            });
            d.finish(Status::Failed, "GitHub refused", "broker_refused");
            deliveries::record(
                &state.dynamo,
                &state.config.settings_table_name,
                &relay::install_pk(&install.id),
                &d,
            )
            .await;
            Err(fail(
                StatusCode::BAD_GATEWAY,
                "broker_refused",
                &format!("GitHub refused the token: {msg}"),
            ))
        }
    }
}

/// The relay's part of `POST /webhooks/github` on the hosted side: when the
/// installation is linked to an install, forward the (already verified)
/// event there instead of handling it here. Returns None when it isn't.
pub async fn relay_github(
    state: &AppState,
    installation_id: u64,
    event_type: &str,
    action: &str,
    headers: &HeaderMap,
    body: &Bytes,
) -> Option<StatusCode> {
    let master = master(state)?;
    let install_id = install_for_installation(state, installation_id).await?;
    let gh_delivery = headers
        .get("x-github-delivery")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let event = if action.is_empty() {
        event_type.to_string()
    } else {
        format!("{event_type}.{action}")
    };
    let mut d = Delivery::new(Direction::Outbound, "relay", &event, &install_id).with_headers(
        headers
            .iter()
            .filter_map(|(k, v)| Some((k.as_str(), v.to_str().ok()?))),
    );
    // The relay keeps no payload preview: its records are CoderHelm's, and
    // payloads stay out of them.
    d.payload_bytes = body.len();
    d.external_id = gh_delivery.clone();
    d.redeliver = Some(deliveries::Redeliver::Relay);
    let pk = relay::install_pk(&install_id);
    let Ok(Some(install)) = load_install(state, &install_id).await else {
        d.finish(Status::Failed, "Not linked", "relay_not_linked");
        deliveries::record(&state.dynamo, &state.config.settings_table_name, &pk, &d).await;
        return Some(StatusCode::OK);
    };
    if !install.verified() {
        d.finish(Status::Failed, "Not linked", "relay_not_linked");
        deliveries::record(&state.dynamo, &state.config.settings_table_name, &pk, &d).await;
        return Some(StatusCode::OK);
    }
    if event_type == "installation" && action == "deleted" {
        unlink(state, &install_id, installation_id).await;
    }
    let delivery_id = if gh_delivery.is_empty() {
        d.id.clone()
    } else {
        gh_delivery
    };
    forward_and_record(
        state,
        master,
        &install,
        d,
        "github",
        event_type,
        &delivery_id,
        body,
    )
    .await;
    // GitHub hears 200 either way: the relay owns the retries now.
    Some(StatusCode::OK)
}

/// Forwards one event to a linked install now. When that fails, the body is
/// held in the data bucket and the first retry queued (the worker's
/// `relay_retry` keeps going for about 24 hours, then gives up; a job that
/// keeps failing goes to the dead-letter queue like any other). Records the
/// delivery and the provider's last forward either way. GitHub and Jira
/// forwards go through here alike.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn forward_and_record(
    state: &AppState,
    master: &str,
    install: &Install,
    mut d: Delivery,
    source: &str,
    event: &str,
    delivery_id: &str,
    body: &[u8],
) -> relay::Next {
    let pk = relay::install_pk(&install.id);
    let key = sign::relay_key(master, &install.id);
    let origin = install.origin();
    let attempt = relay::send(&relay::Forward {
        origin: &origin,
        relay_key: &key,
        source,
        event,
        delivery_id,
        body,
    })
    .await;
    let next = relay::apply(&mut d, attempt);
    if let relay::Next::RetryIn(secs) = next {
        // Hold the body for the retries, then queue the first one.
        let held = state
            .s3
            .put_object()
            .bucket(&state.config.bucket_name)
            .key(relay::payload_key(&d.id))
            .body(body.to_vec().into())
            .send()
            .await
            .is_ok();
        let msg = json!({ "type": "relay_retry", "install_id": install.id, "delivery_id": d.id, "relay_delivery_id": delivery_id, "event": event, "source": source });
        let queued = held
            && state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(msg.to_string())
                .delay_seconds(secs.min(900) as i32)
                .send()
                .await
                .is_ok();
        if !queued {
            d.finish(
                Status::Failed,
                "Couldn't schedule a retry",
                "relay_unreachable",
            );
            d.next_retry_at = None;
        }
    }
    deliveries::record(&state.dynamo, &state.config.settings_table_name, &pk, &d).await;
    relay::note_last(
        &state.dynamo,
        &state.config.settings_table_name,
        &install.id,
        &d,
    )
    .await;
    next
}

/// The relay's part of the hosted Jira Forge intake: a Forge event whose
/// team id is an install id goes to that install, unchanged. The install
/// checks the Forge secret itself, so this side can't make one up. Retried
/// like a GitHub forward; Forge hears 200 once the relay owns the event.
pub async fn relay_jira(
    state: &AppState,
    install_id: &str,
    path_event: &str,
    body: &[u8],
) -> Option<StatusCode> {
    let master = master(state)?;
    if !sign::is_install_id(install_id) {
        return None;
    }
    let pk = relay::install_pk(install_id);
    let mut d = Delivery::new(
        Direction::Outbound,
        relay::source_for("jira"),
        &format!("jira.{path_event}"),
        install_id,
    );
    d.payload_bytes = body.len();
    d.redeliver = Some(deliveries::Redeliver::Relay);
    let install = match load_install(state, install_id).await {
        Ok(Some(i)) if i.verified() && i.integrations.iter().any(|x| x == "jira") => i,
        _ => {
            d.finish(Status::Failed, "Not linked", "relay_not_linked");
            deliveries::record(&state.dynamo, &state.config.settings_table_name, &pk, &d).await;
            return Some(StatusCode::NOT_FOUND);
        }
    };
    let id = d.id.clone();
    forward_and_record(state, master, &install, d, "jira", path_event, &id, body).await;
    Some(StatusCode::OK)
}

// ── Relay and broker deliveries for the install ──────────────────────────

/// GET /hybrid/deliveries (signed): this install's relay and broker records,
/// for its own Deliveries page.
pub async fn list_deliveries(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    let items = deliveries::list(
        &state.dynamo,
        &state.config.settings_table_name,
        &relay::install_pk(&install.id),
        &deliveries::Filter::default(),
        200,
    )
    .await
    .map_err(|_| {
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "store_unavailable",
            "Try again in a minute.",
        )
    })?;
    let own: Vec<_> = items
        .into_iter()
        .filter(|d| relay::is_relay_source(&d.source) || d.source == "broker")
        .collect();
    Ok(Json(json!({ "deliveries": own })))
}

/// POST /hybrid/deliveries/rollups (signed) `{from, to}`: this install's
/// relay and broker counters between two rollup sort keys, for its own
/// Deliveries overview. A POST so the signature covers the range.
pub async fn rollups(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    let b = body_json(&body);
    let (Some(from), Some(to)) = (b["from"].as_str(), b["to"].as_str()) else {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "Send {from, to}.",
        ));
    };
    Ok(Json(
        super::deliveries_analytics::install_rollups(
            &state,
            &relay::install_pk(&install.id),
            from,
            to,
        )
        .await,
    ))
}

/// POST /hybrid/deliveries/:id/redeliver (signed): forward a held event again.
pub async fn redeliver(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    let master = master(&state).ok_or_else(not_here)?.to_string();
    let pk = relay::install_pk(&install.id);
    let mut d = deliveries::get(&state.dynamo, &state.config.settings_table_name, &pk, &id)
        .await
        .ok()
        .flatten()
        .filter(|d| relay::is_relay_source(&d.source))
        .ok_or_else(|| {
            fail(
                StatusCode::NOT_FOUND,
                "not_found",
                "No such relay delivery.",
            )
        })?;
    let held = state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(relay::payload_key(&d.id))
        .send()
        .await
        .map_err(|_| {
            let from = if relay::provider_of(&d.source) == Some("jira") {
                "Make the change in Jira again (edit the issue or add the comment)."
            } else {
                "Redeliver it from GitHub: the app's Advanced settings list recent deliveries."
            };
            fail(
                StatusCode::GONE,
                "payload_gone",
                &format!(
                    "The event isn't held any more (kept up to 24 hours while retrying). {from}"
                ),
            )
        })?;
    let bytes = held
        .body
        .collect()
        .await
        .map(|b| b.into_bytes())
        .unwrap_or_default();
    let (source, event) = relay::resend_route(&d);
    // A new id for the install's replay check, in the provider's own
    // namespace (Jira's ids start `jira-`).
    let base = if d.external_id.is_empty() {
        &d.id
    } else {
        &d.external_id
    };
    let rid = if source == "jira" && !base.starts_with("jira-") {
        format!("jira-{base}-r{}", d.attempts.len())
    } else {
        format!("{base}-r{}", d.attempts.len())
    };
    let key = sign::relay_key(&master, &install.id);
    let origin = install.origin();
    let a = relay::send(&relay::Forward {
        origin: &origin,
        relay_key: &key,
        source,
        event: &event,
        delivery_id: &rid,
        body: &bytes,
    })
    .await;
    let next = relay::apply(&mut d, a);
    if matches!(next, relay::Next::Delivered | relay::Next::AlreadyDelivered) {
        let _ = state
            .s3
            .delete_object()
            .bucket(&state.config.bucket_name)
            .key(relay::payload_key(&d.id))
            .send()
            .await;
    }
    deliveries::record(&state.dynamo, &state.config.settings_table_name, &pk, &d).await;
    relay::note_last(
        &state.dynamo,
        &state.config.settings_table_name,
        &install.id,
        &d,
    )
    .await;
    Ok(Json(json!({ "delivery": d })))
}

/// POST /hybrid/deliveries/shared (signed): failed-delivery metadata an
/// install shares with support. Kept only with sharing on; payload previews
/// only when the install chose to share them too.
pub async fn share(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    if !install.sharing {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "sharing_off",
            "Sharing is off for this install.",
        ));
    }
    let mut d: Delivery = serde_json::from_value(body_json(&body)["delivery"].clone())
        .map_err(|_| fail(StatusCode::BAD_REQUEST, "bad_request", "Send {delivery}."))?;
    if !install.share_payloads {
        d.payload_preview.clear();
    }
    d.headers.clear();
    d.source = format!("shared:{}", d.source);
    deliveries::record(
        &state.dynamo,
        &state.config.settings_table_name,
        &relay::install_pk(&install.id),
        &d,
    )
    .await;
    Ok(Json(json!({ "ok": true })))
}

// ── OAuth broker (Notion, Confluence, Google Drive) ──────────────────────

fn kind_of(raw: &str) -> Option<common::knowledge::sources::Kind> {
    use common::knowledge::sources::Kind;
    match raw {
        "notion" => Some(Kind::Notion),
        "confluence" => Some(Kind::Confluence),
        "google_drive" | "google-drive" => Some(Kind::GoogleDrive),
        _ => None,
    }
}

/// The hosted integrations this side can actually serve: a knowledge
/// provider only when CoderHelm's own app for it is set up (Google Drive
/// needs `google_drive_client_id` and `google_drive_client_secret`, never
/// the Google sign-in client). Installs see this as `offered` and can't
/// choose the rest.
pub fn offered_ids(state: &AppState) -> Vec<&'static str> {
    offered_from(|k| super::knowledge_sources::app(state, k).is_set())
}

/// [`offered_ids`] for a given "is this provider's app set up" (pure; tested).
pub fn offered_from(
    app_set: impl Fn(common::knowledge::sources::Kind) -> bool,
) -> Vec<&'static str> {
    common::hybrid::integrations::hosted_ids()
        .filter(|id| kind_of(id).is_none_or(&app_set))
        .collect()
}

fn hosted_team(install: &str) -> String {
    relay::install_pk(install)
}

/// The holding partition one provider sign-in is finished in. It moves into
/// the install's own ([`hosted_team`]) only when the install redeems the
/// code with its secret and the PKCE verifier ([`oauth_token`]): the
/// authorize link is public, so whoever follows it could otherwise put their
/// own provider account (or nothing) in place of the install's.
pub fn signin_team(install: &str, flow: &str) -> String {
    format!(
        "{}{}{flow}",
        relay::install_pk(install),
        common::mcp_remote::store::HOLDING_MARKER
    )
}

/// A flow id: what [`signin_team`] is keyed by.
fn is_flow(flow: &str) -> bool {
    flow.strip_prefix("fl_")
        .is_some_and(|t| t.len() == 40 && t.chars().all(|c| c.is_ascii_alphanumeric()))
}

#[derive(Deserialize, Default)]
pub struct AuthorizeQuery {
    client_id: Option<String>,
    redirect_uri: Option<String>,
    state: Option<String>,
    code_challenge: Option<String>,
    code_challenge_method: Option<String>,
}

/// GET /hybrid/oauth/:kind/authorize: the install's sign-in starts here (a
/// standard authorization request with PKCE, client id = install id). This
/// side checks the install and its redirect, then sends the person on to
/// the provider with CoderHelm's own app.
pub async fn oauth_authorize(
    State(state): State<Arc<AppState>>,
    Path(kind): Path<String>,
    ip: ClientIp,
    Query(q): Query<AuthorizeQuery>,
) -> Response {
    if master(&state).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    // Public: each start writes a holding partition, so it is limited per address.
    if rate_limit::allow(&ip, "hybrid-authorize", 20, 600).is_err() {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "Too many sign-ins started from here. Try again in a few minutes.",
        )
            .into_response();
    }
    let Some(k) = kind_of(&kind) else {
        return (StatusCode::NOT_FOUND, "Unknown provider.").into_response();
    };
    let install_id = q.client_id.unwrap_or_default();
    let install = match load_install(&state, &install_id).await {
        Ok(Some(i)) if i.verified() && i.integrations.iter().any(|x| x == k.key()) => i,
        _ => return (StatusCode::BAD_REQUEST, "This install hasn't turned on CoderHelm's app for this provider. Turn it on in Settings, System, Hybrid.").into_response(),
    };
    let redirect = q.redirect_uri.unwrap_or_default();
    if !broker::redirect_allowed(&redirect, &install.origins()) {
        return (
            StatusCode::BAD_REQUEST,
            "The return address isn't this install's.",
        )
            .into_response();
    }
    let (Some(install_state), Some(challenge)) = (q.state, q.code_challenge) else {
        return (StatusCode::BAD_REQUEST, "Missing state or code_challenge.").into_response();
    };
    if q.code_challenge_method.as_deref() != Some("S256") {
        return (
            StatusCode::BAD_REQUEST,
            "Only S256 code challenges are accepted.",
        )
            .into_response();
    }
    // The provider sign-in itself: CoderHelm's app, finished in a holding
    // partition of its own; the install's partition is never touched here.
    let flow = broker::new_handle("fl");
    let team = signin_team(&install.id, &flow);
    let rt = super::mcp_servers::runtime(&state, &team);
    let a = super::knowledge_sources::app(&state, k);
    let Some(p) = common::knowledge::sources::oauth::provider(k) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !a.is_set() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "CoderHelm's app for this provider isn't set up.",
        )
            .into_response();
    }
    let id = common::knowledge::sources::oauth::connection_id(k);
    let Ok(sealed) = rt.seal_client_secret(&id, a.client_secret.trim()).await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let mut rec =
        common::knowledge::sources::oauth::connection_record(k, &p, a.client_id.trim(), &sealed);
    rec.added_by = format!("hybrid:{}", install.id);
    rec.added_at = now();
    if rt.store.put_server(&team, &rec).await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let sub = format!("hybrid:{}", install.id);
    let url = match rt
        .start_sign_in(
            &mut rec,
            &common::mcp_remote::store::Owner::Team,
            &sub,
            &sub,
            &super::mcp_servers::redirect_uri(&state.config.stage),
            None,
        )
        .await
    {
        Ok(u) => u,
        Err(e) => return (StatusCode::BAD_GATEWAY, e).into_response(),
    };
    // Remember which install this provider sign-in belongs to, by its state.
    let provider_state = url::Url::parse(&url)
        .ok()
        .and_then(|u| {
            u.query_pairs()
                .find(|(k, _)| k == "state")
                .map(|(_, v)| v.to_string())
        })
        .unwrap_or_default();
    let key = format!(
        "HYBRIDOAUTH#{}",
        hex::encode(sha2_digest(provider_state.as_bytes()))
    );
    let fields = [
        ("install_id", install.id.as_str()),
        ("kind", k.key()),
        ("redirect_uri", redirect.as_str()),
        ("install_state", install_state.as_str()),
        ("code_challenge", challenge.as_str()),
        ("flow", flow.as_str()),
    ];
    if put_state(&state, &key, &fields, 900).await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    Redirect::to(&url).into_response()
}

fn sha2_digest(b: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    sha2::Sha256::digest(b).to_vec()
}

/// POST /hybrid/oauth/callback `{code, state, error?, iss?}`: the hosted
/// dashboard's sign-in callback page sends a provider's answer here first.
/// When it belongs to a hybrid install, this side finishes the provider
/// sign-in, then answers where to send the browser: the install's own
/// callback page with a one-time code. Anything else is a 404 and the page
/// carries on as usual.
pub async fn oauth_callback(State(state): State<Arc<AppState>>, body: Bytes) -> Reply {
    master(&state).ok_or_else(not_here)?;
    let b = body_json(&body);
    let provider_state = b["state"].as_str().unwrap_or("");
    let key = format!(
        "HYBRIDOAUTH#{}",
        hex::encode(sha2_digest(provider_state.as_bytes()))
    );
    let pending = take_state(&state, &key).await.ok_or_else(not_here)?;
    let g = |k: &str| {
        pending
            .get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let (install_id, kind, redirect, install_state, challenge, flow) = (
        g("install_id"),
        g("kind"),
        g("redirect_uri"),
        g("install_state"),
        g("code_challenge"),
        g("flow"),
    );
    // A flow started before holding partitions existed has none: start again.
    if !is_flow(&flow) {
        return Err(not_here());
    }
    let back = |q: &str| {
        format!(
            "{redirect}?{q}&state={}",
            urlencoding::encode(&install_state)
        )
    };
    if let Some(err) = b["error"].as_str().filter(|e| !e.is_empty()) {
        return Ok(Json(
            json!({ "redirect": back(&format!("error={}", urlencoding::encode(err))) }),
        ));
    }
    let team = signin_team(&install_id, &flow);
    let rt = super::mcp_servers::runtime(&state, &team);
    let sub = format!("hybrid:{install_id}");
    if let Err(e) = rt
        .finish_sign_in(
            provider_state,
            b["code"].as_str(),
            None,
            b["iss"].as_str(),
            &sub,
            &sub,
        )
        .await
    {
        warn!(install = %install_id, error = %e, "hybrid: provider sign-in failed");
        return Ok(Json(json!({ "redirect": back("error=access_denied") })));
    }
    let code = broker::new_handle("hc");
    put_state(
        &state,
        &format!("HYBRIDCODE#{code}"),
        &[
            ("install_id", &install_id),
            ("kind", &kind),
            ("code_challenge", &challenge),
            ("redirect_uri", &redirect),
            ("flow", &flow),
        ],
        300,
    )
    .await?;
    audit(&state, &install_id, "oauth_signin", json!({ "kind": kind })).await;
    Ok(Json(
        json!({ "redirect": back(&format!("code={}", urlencoding::encode(&code))) }),
    ))
}

/// Moves a finished provider sign-in from its holding partition into the
/// install's own, with CoderHelm's app as the connection there.
async fn adopt_provider_sign_in(
    state: &AppState,
    install_id: &str,
    kind: &str,
    flow: &str,
) -> Result<(), String> {
    if !is_flow(flow) {
        return Err("no holding partition".into());
    }
    let k = kind_of(kind).ok_or("unknown provider")?;
    let p = common::knowledge::sources::oauth::provider(k).ok_or("unknown provider")?;
    let a = super::knowledge_sources::app(state, k);
    if !a.is_set() {
        return Err("CoderHelm's app for this provider isn't set up".into());
    }
    let team = hosted_team(install_id);
    let rt = super::mcp_servers::runtime(state, &team);
    let hold = super::mcp_servers::runtime(state, &signin_team(install_id, flow));
    let id = common::knowledge::sources::oauth::connection_id(k);
    let sealed = rt.seal_client_secret(&id, a.client_secret.trim()).await?;
    let mut rec =
        common::knowledge::sources::oauth::connection_record(k, &p, a.client_id.trim(), &sealed);
    rec.added_by = format!("hybrid:{install_id}");
    rec.added_at = now();
    rt.store.put_server(&team, &rec).await?;
    rt.adopt_sign_in(&hold, &id, &common::mcp_remote::store::Owner::Team)
        .await
}

fn token_error(code: &str, desc: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": code, "error_description": desc })),
    )
}

/// The install's client credentials at the token endpoint (constant time).
pub fn client_ok(master: &str, install_id: &str, secret: &str) -> bool {
    if !sign::is_install_id(install_id) {
        return false;
    }
    let want = sign::install_secret(master, install_id);
    want.len() == secret.len()
        && subtle::ConstantTimeEq::ct_eq(want.as_bytes(), secret.as_bytes()).into()
}

fn form(body: &[u8]) -> HashMap<String, String> {
    url::form_urlencoded::parse(body).into_owned().collect()
}

/// POST /hybrid/oauth/token: the install's token endpoint (form body,
/// client id = install id, client secret = its registration secret).
/// `authorization_code` (with the PKCE verifier) answers an access token and
/// a grant handle; `refresh_token` with the handle answers a fresh access
/// token. The provider's own refresh token never leaves this side.
pub async fn oauth_token(State(state): State<Arc<AppState>>, body: Bytes) -> Reply {
    let master = master(&state).ok_or_else(not_here)?.to_string();
    let f = form(&body);
    let g = |k: &str| f.get(k).cloned().unwrap_or_default();
    let install_id = g("client_id");
    if !client_ok(&master, &install_id, &g("client_secret")) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "invalid_client" })),
        ));
    }
    let install = load_install(&state, &install_id)
        .await?
        .filter(Install::verified)
        .ok_or_else(|| token_error("invalid_client", "Unknown or turned off install."))?;
    let (kind, handle) = match g("grant_type").as_str() {
        "authorization_code" => {
            let c = take_state(&state, &format!("HYBRIDCODE#{}", g("code")))
                .await
                .ok_or_else(|| token_error("invalid_grant", "The code was used or expired."))?;
            let cg = |k: &str| {
                c.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            if cg("install_id") != install.id || cg("redirect_uri") != g("redirect_uri") {
                return Err(token_error(
                    "invalid_grant",
                    "The code belongs to another install or return address.",
                ));
            }
            if !broker::pkce_matches(&cg("code_challenge"), &g("code_verifier")) {
                return Err(token_error(
                    "invalid_grant",
                    "The code verifier doesn't match.",
                ));
            }
            let kind = cg("kind");
            // Only now, with the install's secret and verifier checked, does
            // the sign-in become the install's.
            adopt_provider_sign_in(&state, &install.id, &kind, &cg("flow"))
                .await
                .map_err(|e| {
                    warn!(install = %install.id, error = %e, "hybrid: sign-in not taken");
                    token_error(
                        "invalid_grant",
                        "The sign-in couldn't be finished. Start it again.",
                    )
                })?;
            let handle = broker::new_handle("hr");
            state
                .dynamo
                .put_item()
                .table_name(&state.config.settings_table_name)
                .item("pk", s(&broker::grant_key(&handle)))
                .item("sk", s("GRANT"))
                .item("install_id", s(&install.id))
                .item("kind", s(&kind))
                .item("created_at", s(&now()))
                .send()
                .await
                .map_err(|_| token_error("temporarily_unavailable", "Try again."))?;
            (kind, handle)
        }
        "refresh_token" => {
            let handle = g("refresh_token");
            let r = state
                .dynamo
                .get_item()
                .table_name(&state.config.settings_table_name)
                .key("pk", s(&broker::grant_key(&handle)))
                .key("sk", s("GRANT"))
                .send()
                .await
                .map_err(|_| token_error("temporarily_unavailable", "Try again."))?;
            let item = r.item().ok_or_else(|| {
                token_error("invalid_grant", "That sign-in was revoked. Reconnect.")
            })?;
            let gi = |k: &str| {
                item.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            if gi("install_id") != install.id {
                return Err(token_error(
                    "invalid_grant",
                    "That sign-in belongs to another install.",
                ));
            }
            (gi("kind"), handle)
        }
        _ => {
            return Err(token_error(
                "unsupported_grant_type",
                "Use authorization_code or refresh_token.",
            ))
        }
    };
    if !install.integrations.contains(&kind) {
        return Err(token_error(
            "invalid_grant",
            "CoderHelm's app for this provider was turned off for this install.",
        ));
    }
    let k = kind_of(&kind).ok_or_else(|| token_error("invalid_grant", "Unknown provider."))?;
    let team = hosted_team(&install.id);
    let rt = super::mcp_servers::runtime(&state, &team);
    let rec = rt
        .store
        .get_server(&team, &common::knowledge::sources::oauth::connection_id(k))
        .await
        .ok()
        .flatten()
        .ok_or_else(|| token_error("invalid_grant", "No sign-in for this provider. Reconnect."))?;
    let header = rt
        .auth_header(&rec, &common::mcp_remote::store::Owner::Team, false)
        .await
        .map_err(|e| token_error("invalid_grant", &format!("{e:?}")))?
        .ok_or_else(|| token_error("invalid_grant", "No sign-in for this provider. Reconnect."))?;
    let access = header
        .1
        .strip_prefix("Bearer ")
        .unwrap_or(&header.1)
        .to_string();
    let mut d = Delivery::new(
        Direction::Outbound,
        "broker",
        &format!("{kind}.token"),
        &install.id,
    );
    d.finish(Status::Handled, "Token handed out", "");
    deliveries::record(
        &state.dynamo,
        &state.config.settings_table_name,
        &relay::install_pk(&install.id),
        &d,
    )
    .await;
    // Ten minutes: the install asks again with the handle after that.
    Ok(Json(
        json!({ "access_token": access, "token_type": "Bearer", "expires_in": 600, "refresh_token": handle }),
    ))
}

/// POST /hybrid/oauth/revoke `token=<handle>` (client authenticated): forget a grant.
pub async fn oauth_revoke(State(state): State<Arc<AppState>>, body: Bytes) -> Reply {
    let master = master(&state).ok_or_else(not_here)?.to_string();
    let f = form(&body);
    let install_id = f.get("client_id").cloned().unwrap_or_default();
    if !client_ok(
        &master,
        &install_id,
        f.get("client_secret").map(String::as_str).unwrap_or(""),
    ) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "invalid_client" })),
        ));
    }
    if let Some(t) = f.get("token") {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", s(&broker::grant_key(t)))
            .key("sk", s("GRANT"))
            .condition_expression("install_id = :i")
            .expression_attribute_values(":i", s(&install_id))
            .send()
            .await;
        audit(&state, &install_id, "oauth_revoke", json!({})).await;
    }
    Ok(Json(json!({})))
}

// ── Admin (CoderHelm's own operators) ────────────────────────────────────

pub(crate) fn operator(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    if !super::api::is_platform_admin(claims) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Platform admins only.",
        ));
    }
    Ok(())
}

/// GET /api/admin/hybrid: every registered install and the latest audit lines.
pub async fn admin_list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    operator(&claims)?;
    master(&state).ok_or_else(not_here)?;
    let q = |pk: &'static str, limit: i32| {
        state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p")
            .expression_attribute_values(":p", s(pk))
            .scan_index_forward(false)
            .limit(limit)
            .send()
    };
    let installs = q("HYBRIDINSTALLS", 500)
        .await
        .map(|r| r.items().to_vec())
        .unwrap_or_default();
    let audit_lines = q("HYBRIDAUDIT", 100)
        .await
        .map(|r| r.items().to_vec())
        .unwrap_or_default();
    let flat = |i: &HashMap<String, AttributeValue>| {
        let mut o = serde_json::Map::new();
        for (k, v) in i {
            if let Ok(t) = v.as_s() {
                o.insert(k.clone(), json!(t));
            }
        }
        Value::Object(o)
    };
    Ok(Json(json!({
        "installs": installs.iter().map(flat).collect::<Vec<_>>(),
        "audit": audit_lines.iter().map(flat).collect::<Vec<_>>(),
    })))
}

/// POST /api/admin/hybrid/:install_id/revoke `{reason}`: turn an install's
/// access off. Its name and its records go, its links stop relaying, and the
/// broker refuses it from now on.
pub async fn admin_revoke(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    operator(&claims)?;
    master(&state).ok_or_else(not_here)?;
    let mut install = load_install(&state, &id)
        .await?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "not_found", "No such install."))?;
    release_name(&state, &mut install).await?;
    // Its GitHub installations stop relaying here and are free again.
    for l in github_links(&state, &install.id).await {
        unlink(&state, &install.id, l.installation_id).await;
    }
    super::hybrid_jira::unlink_all(&state, &install.id).await;
    install.status = "revoked".into();
    install.integrations.clear();
    save_install(&state, &install).await?;
    // And the same CloudFront can't simply register again.
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(&revoked_cloudfront_key(&install.cloudfront)))
        .item("sk", s("REVOKED"))
        .item("install_id", s(&install.id))
        .item("at", s(&now()))
        .send()
        .await
        .map_err(|_| {
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?;
    audit(
        &state,
        &id,
        "revoke",
        json!({ "by": claims.email, "reason": body["reason"].as_str().unwrap_or("") }),
    )
    .await;
    Ok(Json(install.view()))
}

#[cfg(test)]
mod tests {

    #[test]
    fn drive_is_offered_to_installs_only_with_its_own_app() {
        use common::knowledge::sources::Kind;
        let none = offered_from(|_| false);
        assert_eq!(none, vec!["github", "jira"]);
        let no_drive = offered_from(|k| k != Kind::GoogleDrive);
        assert!(!no_drive.contains(&"google_drive"));
        assert!(no_drive.contains(&"notion") && no_drive.contains(&"confluence"));
        assert!(offered_from(|_| true).contains(&"google_drive"));
    }

    use super::*;

    /// Linking writes the install's link and the installation's reverse
    /// index in one transaction; the reverse index carries the condition,
    /// so a link held by another install fails both.
    #[test]
    fn a_link_and_its_reverse_index_are_written_together() {
        let link = broker::GithubLink {
            installation_id: 42,
            account: "example-org".into(),
            repositories: vec![],
            ceiling: Some(vec!["web".into()]),
        };
        let items = link_writes("settings", "ins_abcdefghijklmnopqrstuvwx", &link).unwrap();
        assert_eq!(items.len(), 2);
        let own = items[0].put().unwrap();
        assert_eq!(
            own.item()["pk"],
            s(&relay::install_pk("ins_abcdefghijklmnopqrstuvwx"))
        );
        assert_eq!(own.item()["sk"], s("GHLINK#42"));
        assert!(own.condition_expression().is_none());
        let reverse = items[1].put().unwrap();
        assert_eq!(reverse.item()["pk"], s("HYBRIDGH#42"));
        assert_eq!(
            reverse.condition_expression(),
            Some("attribute_not_exists(pk) OR install_id = :i")
        );
        assert_eq!(
            reverse.expression_attribute_values().unwrap()[":i"],
            s("ins_abcdefghijklmnopqrstuvwx")
        );
    }

    #[test]
    fn an_install_round_trips_through_its_item() {
        let i = Install {
            id: "ins_abcdefghijklmnopqrstuvwx".into(),
            status: "active".into(),
            cloudfront: "d1abcdef234567.cloudfront.net".into(),
            verified_at: "2026-10-25T00:00:00Z".into(),
            name: "example".into(),
            created_at: "2026-10-24T00:00:00Z".into(),
            version: "v1.2.3".into(),
            sharing: true,
            share_payloads: false,
            integrations: vec!["github".into(), "notion".into()],
            name_changes: vec![1, 2],
        };
        assert_eq!(Install::from_item(&i.to_item()).unwrap(), i);
        assert!(i.verified());
        assert_eq!(
            i.origins(),
            vec![
                "https://d1abcdef234567.cloudfront.net",
                "https://example.coderhelm.com"
            ]
        );
        let v = i.view();
        assert_eq!(v["hostname"], "example.coderhelm.com");
        assert_eq!(v["sharing"]["payloads"], false);
        let mut off = i.clone();
        off.status = "revoked".into();
        assert!(!off.verified());
    }

    #[test]
    fn name_changes_are_limited_per_day() {
        let now = 1_800_000_000;
        assert!(name_change_allowed(&[], now));
        let four: Vec<i64> = (0..4).map(|i| now - i * 100).collect();
        assert!(name_change_allowed(&four, now));
        let five: Vec<i64> = (0..5).map(|i| now - i * 100).collect();
        assert!(!name_change_allowed(&five, now));
        let old: Vec<i64> = (0..5).map(|i| now - 90_000 - i).collect();
        assert!(name_change_allowed(&old, now));
    }

    #[test]
    fn linking_needs_the_person_to_see_the_installation() {
        let v = json!({"installations": [
            {"id": 5, "app_id": 9, "account": {"login": "example-org"}},
            {"id": 6, "app_id": 9, "account": {"login": "other"}},
            {"id": 8, "app_id": 1234, "account": {"login": "another-app"}}
        ]});
        assert_eq!(
            user_sees_installation(&v, 5, 9).as_deref(),
            Some("example-org")
        );
        assert_eq!(user_sees_installation(&v, 7, 9), None);
        assert_eq!(user_sees_installation(&json!({}), 5, 9), None);
        // Another app's installation never counts, nor does an unknown app id.
        assert_eq!(user_sees_installation(&v, 8, 9), None);
        assert_eq!(user_sees_installation(&v, 5, 0), None);
    }

    #[test]
    fn the_return_address_is_one_of_the_installs_own_hosts() {
        let i = Install {
            id: "ins_abcdefghijklmnopqrstuvwx".into(),
            status: "active".into(),
            cloudfront: "d1abcdef234567.cloudfront.net".into(),
            name: "example".into(),
            ..Default::default()
        };
        let home = "https://d1abcdef234567.cloudfront.net/settings/system/#hybrid";
        for ok in [
            "https://d1abcdef234567.cloudfront.net/settings/system/?x=1#hybrid",
            "https://example.coderhelm.com/settings/system/",
            "https://docs.coderhelm.com/self-host/install/",
        ] {
            assert_eq!(
                return_url_for(&i, ok),
                url::Url::parse(ok).unwrap().to_string(),
                "{ok}"
            );
        }
        for bad in [
            "https://d1abcdef234567.cloudfront.net.example.org/x",
            "https://d1abcdef234567.cloudfront.net@example.org/x",
            "https://example.coderhelm.com.example.org/",
            "https://docs.coderhelm.com.example.org/",
            "http://d1abcdef234567.cloudfront.net/",
            "https://d1abcdef234567.cloudfront.net:8443/",
            "javascript:alert(1)",
            "//example.org",
            "",
        ] {
            assert_eq!(return_url_for(&i, bad), home, "{bad}");
        }
    }

    #[test]
    fn the_link_confirmation_names_the_install_and_cant_be_framed() {
        let r = confirm_page("example-org", "example.coderhelm.com", 3, "hl_abc");
        assert_eq!(r.status(), StatusCode::OK);
        let h = r.headers();
        assert_eq!(h.get("x-frame-options").unwrap(), "DENY");
        assert!(h
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("frame-ancestors 'none'"));
        assert_eq!(h.get("cache-control").unwrap(), "no-store");
        let r = confirm_page("<b>x</b>", "a\"b", 1, "hl_\"><script>");
        let body = futures_body(r);
        assert!(!body.contains("<b>x</b>") && !body.contains("<script>"));
        assert!(body.contains("&lt;b&gt;x&lt;/b&gt;") && body.contains("1 repository"));
    }

    fn futures_body(r: Response) -> String {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let b = rt
            .block_on(axum::body::to_bytes(r.into_body(), 1 << 20))
            .unwrap();
        String::from_utf8(b.to_vec()).unwrap()
    }

    #[test]
    fn returns_carry_the_outcome_before_the_fragment() {
        let r = page_redirect(
            "https://d1abcdef234567.cloudfront.net/settings/system/#hybrid",
            "linked",
        );
        let loc = r
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(
            loc,
            "https://d1abcdef234567.cloudfront.net/settings/system/?hybrid_github=linked#hybrid"
        );
        let r = page_redirect(
            "https://docs.coderhelm.com/self-host/install/?id=x",
            "not_yours",
        );
        assert!(r
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap()
            .ends_with("?id=x&hybrid_github=not_yours"));
    }

    #[test]
    fn client_credentials_are_the_derived_secret() {
        let m = "m".repeat(40);
        let id = "ins_abcdefghijklmnopqrstuvwx";
        assert!(client_ok(&m, id, &sign::install_secret(&m, id)));
        assert!(!client_ok(&m, id, "wrong"));
        assert!(!client_ok(
            &m,
            "ins_bad",
            &sign::install_secret(&m, "ins_bad")
        ));
    }

    #[test]
    fn fallback_pages_have_the_right_status_and_never_get_indexed() {
        let (st, h, body) = fallback_response(FallbackPage::Nobody, "/anything");
        assert_eq!(st, StatusCode::NOT_FOUND);
        assert!(h.contains(&("x-robots-tag", "noindex, nofollow")));
        assert!(body.contains("noindex") && body.contains("no CoderHelm here yet"));
        assert!(body.contains("/self-host/install/"));
        let (st, h, body) = fallback_response(FallbackPage::SettingUp, "/");
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
        assert!(h.iter().any(|(k, _)| *k == "retry-after"));
        assert!(body.contains("still being set up"));
        let (st, h, body) = fallback_response(FallbackPage::Nobody, "/robots.txt");
        assert_eq!(st, StatusCode::OK);
        assert!(body.contains("Disallow: /"));
        assert!(h.contains(&("content-type", "text/plain; charset=utf-8")));
    }

    /// FALLBACK_PAGES_DIR=<dir> writes the two pages, for screenshots.
    #[test]
    fn fallback_pages_can_be_written_for_review() {
        let Ok(dir) = std::env::var("FALLBACK_PAGES_DIR") else {
            return;
        };
        for (page, name) in [
            (FallbackPage::Nobody, "nobody"),
            (FallbackPage::SettingUp, "setting-up"),
        ] {
            let (_, _, body) = fallback_response(page, "/");
            std::fs::write(format!("{dir}/{name}.html"), body).unwrap();
        }
    }

    #[test]
    fn fallback_hosts_are_one_label_under_the_zone() {
        assert_eq!(
            name_from_host("Example.coderhelm.com").as_deref(),
            Some("example")
        );
        assert_eq!(
            name_from_host("example.coderhelm.com:443").as_deref(),
            Some("example")
        );
        assert_eq!(name_from_host("a.b.coderhelm.com"), None);
        assert_eq!(name_from_host("coderhelm.com"), None);
        assert_eq!(name_from_host("example.org"), None);
    }

    /// Customer installs serve their own pages on `{name}.coderhelm.com`, the
    /// same site as app.coderhelm.com. Every cookie the gateway sets must
    /// stay host-only (`__Host-`, no Domain), so no customer subdomain can
    /// read or overwrite a session or a sign-in state.
    #[test]
    fn every_cookie_is_host_only() {
        fn walk(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
            for e in std::fs::read_dir(dir).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push((
                        p.display().to_string(),
                        std::fs::read_to_string(&p).unwrap(),
                    ));
                }
            }
        }
        let mut files = vec![];
        walk(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut files,
        );
        let mut cookie_lines = 0;
        for (path, text) in &files {
            if path.ends_with("routes/hybrid.rs") {
                continue; // this test's own patterns
            }
            for line in text.lines() {
                let l = line.trim();
                if l.starts_with("//")
                    || !(l.contains("; HttpOnly") || l.contains("Path=/; HttpOnly"))
                {
                    continue;
                }
                cookie_lines += 1;
                assert!(
                    !l.to_ascii_lowercase().contains("domain="),
                    "{path}: a cookie with a Domain attribute: {l}"
                );
                let value = l.split('"').nth(1).unwrap_or("");
                let name = value.split('=').next().unwrap_or("");
                assert!(
                    name.starts_with("__Host-") || name.starts_with('{'),
                    "{path}: a cookie that isn't __Host-: {l}"
                );
                assert!(l.contains("Secure"), "{path}: a cookie without Secure: {l}");
            }
        }
        assert!(cookie_lines >= 5, "the scan found the session cookies");
        // The cookie names the format strings use are __Host- too.
        assert!(super::super::support_access::SUPPORT_COOKIE.starts_with("__Host-"));
    }

    #[test]
    fn a_provider_sign_in_is_held_apart_from_the_install() {
        let id = "ins_abcdefghijklmnopqrstuvwx";
        let flow = broker::new_handle("fl");
        assert!(is_flow(&flow));
        let hold = signin_team(id, &flow);
        assert_ne!(hold, hosted_team(id));
        assert!(hold.starts_with(&format!("{}#SIGNIN#fl_", hosted_team(id))));
        // Its records expire by themselves if the install never takes them.
        assert!(common::mcp_remote::store::holding_expiry(&hold, 0).is_some());
        assert!(common::mcp_remote::store::holding_expiry(&hosted_team(id), 0).is_none());
        assert!(!is_flow(""));
        assert!(!is_flow("fl_short"));
        assert!(!is_flow(&format!("fl_{}#x", "a".repeat(38))));
    }

    #[test]
    fn a_revoked_cloudfront_is_one_key_however_it_is_written() {
        assert_eq!(
            revoked_cloudfront_key("D1ABCDEF234567.cloudfront.net."),
            revoked_cloudfront_key("d1abcdef234567.cloudfront.net")
        );
        assert_eq!(
            revoked_cloudfront_key("d1abcdef234567.cloudfront.net"),
            "HYBRIDREVOKED#d1abcdef234567.cloudfront.net"
        );
    }

    #[test]
    fn token_forms_parse() {
        let f = form(b"grant_type=refresh_token&refresh_token=hr_abc&client_id=ins_x");
        assert_eq!(f["grant_type"], "refresh_token");
        assert_eq!(f["refresh_token"], "hr_abc");
    }
}
