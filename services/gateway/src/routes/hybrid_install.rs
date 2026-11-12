//! The install side of hybrid self-hosting: Settings, System, Hybrid, the
//! install guide's hybrid steps, and the relay intake.
//!
//! - `/api/system/hybrid…` (install admins) and `/api/setup/hybrid…` (the
//!   install guide, with the one-time setup key, only until the first person
//!   signs up) register the install with CoderHelm, claim a
//!   `{name}.coderhelm.com` address, link CoderHelm's GitHub App, choose
//!   which integrations use CoderHelm's apps, and choose what delivery data
//!   is shared with support.
//! - `POST /webhooks/relay` takes events CoderHelm's relay forwards: it checks
//!   the signature (the install's relay key), refuses anything older than 5
//!   minutes and any delivery id seen before, then handles the event exactly
//!   as if GitHub (or Jira) had sent it here.
//! - `GET /webhooks/relay/proof` answers CoderHelm's ownership challenge.
//!
//! The registration (id, secret, relay key) lives in the install's own
//! secret; everything else here is the `SYSTEM`/`HYBRID` settings item.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Extension, Json,
};
use common::deliveries::{self, Delivery, Direction, Status};
use common::hybrid::client::{self, Credentials};
use common::hybrid::{hosted_sig, integrations, names, sign};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(code: StatusCode, reason: &str, message: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({ "code": reason, "error": message })))
}

fn admins_only(c: StatusCode) -> (StatusCode, Json<Value>) {
    fail(c, "forbidden", "Admins only.")
}

fn from_call(e: client::CallError) -> (StatusCode, Json<Value>) {
    let code = match e.status {
        None => StatusCode::BAD_GATEWAY,
        Some(s) => StatusCode::from_u16(s).unwrap_or(StatusCode::BAD_GATEWAY),
    };
    fail(code, &e.code, &e.message)
}

// ── The local settings ───────────────────────────────────────────────────

/// What this install chose (the `SYSTEM`/`HYBRID` item).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Local {
    #[serde(default)]
    pub install_id: String,
    #[serde(default)]
    pub cloudfront: String,
    #[serde(default)]
    pub registered_at: String,
    #[serde(default)]
    pub verified_at: String,
    /// The claimed `{name}.coderhelm.com` name.
    #[serde(default)]
    pub name: String,
    /// `own` (the install's GitHub App) or `hosted` (CoderHelm's, relayed).
    #[serde(default)]
    pub github_mode: String,
    /// Integrations using CoderHelm's apps (common::hybrid::integrations), as
    /// CoderHelm last accepted them. Worked out from `github_mode` and
    /// `apps_mode` ([`wanted_integrations`]), never set by hand.
    #[serde(default)]
    pub integrations: Vec<String>,
    /// Other apps, for the whole install: `coderhelm` (CoderHelm's app for
    /// every tool in `integrations::app_choice_ids`, including ones added in
    /// later releases) or `private` (none). A tool with the install's own app
    /// set up uses that app either way. Empty until [`migrate_apps`] runs.
    #[serde(default)]
    pub apps_mode: String,
    /// The per-tool choices of earlier releases (`coderhelm` or `own`). Read
    /// once by [`migrate_apps`]; kept so an older release still finds them.
    #[serde(default)]
    pub apps: BTreeMap<String, String>,
    /// 2 once `apps_mode` holds the choice. 1: the choices were per tool, in
    /// `apps`. 0: they were only in `integrations`.
    #[serde(default)]
    pub apps_v: u8,
    /// The `integrations` last asked of CoderHelm (it may accept fewer:
    /// only what it offers), so the same list isn't sent again.
    #[serde(default)]
    pub apps_sent: Vec<String>,
    #[serde(default)]
    pub sharing: bool,
    #[serde(default)]
    pub share_payloads: bool,
    /// The records last sent for the name (so they're sent once per change).
    #[serde(default)]
    pub records_sent: String,
    /// Settings, Integrations, Jira, "Link through CoderHelm": a hosted team's Jira site
    /// may be linked, and its events taken from the relay. Off by default;
    /// turning it off unlinks.
    #[serde(default)]
    pub jira_link: bool,
    /// The linked site, as last copied from CoderHelm (its `linked_at`
    /// marks which link the copied Forge settings belong to).
    #[serde(default)]
    pub jira_site: String,
    #[serde(default)]
    pub jira_synced_at: String,
}

static LOCAL: RwLock<Option<(Instant, Local)>> = RwLock::new(None);

/// The last settings read in this process (for paths that can't wait on a read).
pub fn cached_local() -> Option<Local> {
    LOCAL
        .read()
        .ok()
        .and_then(|g| g.as_ref().map(|(_, l)| l.clone()))
}

pub async fn local(state: &AppState) -> Local {
    if let Some((at, l)) = LOCAL.read().ok().and_then(|g| g.clone()) {
        if at.elapsed() < Duration::from_secs(30) {
            return l;
        }
    }
    let l: Local = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S("SYSTEM".into()))
        .key("sk", AttributeValue::S("HYBRID".into()))
        .send()
        .await
        .ok()
        .and_then(|r| {
            r.item()
                .and_then(|i| i.get("data"))
                .and_then(|v| v.as_s().ok())
                .cloned()
        })
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default();
    if let Ok(mut g) = LOCAL.write() {
        *g = Some((Instant::now(), l.clone()));
    }
    l
}

async fn save_local(state: &AppState, l: &Local) -> Result<(), (StatusCode, Json<Value>)> {
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", AttributeValue::S("SYSTEM".into()))
        .item("sk", AttributeValue::S("HYBRID".into()))
        .item(
            "data",
            AttributeValue::S(serde_json::to_string(l).unwrap_or_default()),
        )
        .send()
        .await
        .map_err(|e| {
            warn!(error = %e, "hybrid: settings write failed");
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?;
    if let Ok(mut g) = LOCAL.write() {
        *g = Some((Instant::now(), l.clone()));
    }
    Ok(())
}

/// The registration, from memory or (after another instance registered) the secret.
async fn creds_now() -> Option<Credentials> {
    if let Some(c) = client::credentials() {
        return Some(c);
    }
    let s = super::setup::read_secret().await.ok()?;
    let c = Credentials::from_parts(
        s["hybrid_install_id"].as_str(),
        s["hybrid_install_secret"].as_str(),
        s["hybrid_relay_key"].as_str(),
    )?;
    client::set_credentials(Some(c.clone()));
    Some(c)
}

// ── Who may ask ──────────────────────────────────────────────────────────

fn admin(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    claims.require_role(3).map_err(admins_only)?;
    if !super::api::is_platform_admin(claims) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Install admins only.",
        ));
    }
    if !super::setup_checks::self_hosted() {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "not_self_hosted",
            "Hybrid is for self-hosted installs.",
        ));
    }
    if common::hybrid::turned_off() {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "turned_off",
            "Hybrid is turned off in this install's config (hybridUrl: null).",
        ));
    }
    Ok(())
}

/// The install guide: the one-time setup key from the launch, only while
/// nobody has signed up.
async fn setup_key(state: &AppState, headers: &HeaderMap) -> Result<(), (StatusCode, Json<Value>)> {
    if !super::setup::enabled() || common::hybrid::turned_off() {
        return Err(fail(StatusCode::NOT_FOUND, "not_found", "Not found."));
    }
    let got = headers
        .get("x-coderhelm-setup-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let secret = super::setup::read_secret().await.unwrap_or(Value::Null);
    if !super::setup::state_matches(secret.get("setup_token").and_then(Value::as_str), got) {
        return Err(fail(StatusCode::UNAUTHORIZED, "bad_setup_key", "This browser's setup key doesn't match your install. Finish in CoderHelm: Settings, System, Hybrid."));
    }
    if !super::setup_checks::setup_open(state).await {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "setup_closed",
            "Setup is finished. Continue in CoderHelm: Settings, System, Hybrid.",
        ));
    }
    Ok(())
}

// ── Core actions (shared by Settings and the guide) ──────────────────────

/// This install's CloudFront hostname (the relay and the name point there).
async fn cloudfront(state: &AppState) -> Option<String> {
    let (_, view) = super::setup_checks::domain_facts_now(state).await;
    let from_view = view
        .as_ref()
        .and_then(|v| v["target"].as_str())
        .map(|t| t.trim_end_matches('.').to_ascii_lowercase())
        .filter(|t| names::is_cloudfront_domain(t));
    from_view.or_else(|| {
        let u = common::public_urls::app_url(&state.config.stage);
        let host = u
            .trim_start_matches("https://")
            .split('/')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        names::is_cloudfront_domain(&host).then_some(host)
    })
}

/// An answer to a call made before this install has an id (registration, a
/// name lookup), refused unless CoderHelm signed it for this call once the
/// release pins CoderHelm's keys.
async fn signed_answer(
    r: reqwest::Response,
    request_id: &str,
    method: &str,
    path: &str,
) -> Result<Vec<u8>, (StatusCode, Json<Value>)> {
    let status = r.status().as_u16();
    let headers = r.headers().clone();
    let body = r.bytes().await.unwrap_or_default().to_vec();
    client::check_answer(
        hosted_sig::trusted(),
        "",
        request_id,
        status,
        method,
        path,
        &headers,
        &body,
        chrono::Utc::now().timestamp(),
    )
    .map_err(|e| fail(StatusCode::BAD_GATEWAY, &e.code, &e.message))?;
    Ok(body)
}

/// The CoderHelm keys this release pins: each key id and fingerprint, and
/// whether answers and events must carry CoderHelm's signature.
fn coderhelm_keys() -> Value {
    let keys = hosted_sig::trusted();
    json!({
        "required": !keys.is_empty(),
        "source": "pinned in this signed release",
        "keys": keys.describe().into_iter().map(|(kid, fp)| json!({ "kid": kid, "fingerprint": fp })).collect::<Vec<_>>(),
    })
}

pub(crate) async fn do_register(state: &AppState) -> Result<Local, (StatusCode, Json<Value>)> {
    let mut l = local(state).await;
    if creds_now().await.is_some() && !l.install_id.is_empty() {
        return Ok(l);
    }
    let cf = cloudfront(state).await.ok_or_else(|| {
        fail(StatusCode::CONFLICT, "no_cloudfront", "This install's CloudFront address isn't known yet. Wait until it finishes deploying, then try again.")
    })?;
    let version = std::env::var("CODERHELM_VERSION").unwrap_or_default();
    let request_id = hosted_sig::new_request_id();
    let r = state
        .http
        .post(format!("{}/hybrid/installs", common::hybrid::hosted_url()))
        .timeout(Duration::from_secs(10))
        .header(hosted_sig::REQUEST_ID_HEADER, &request_id)
        .json(&json!({ "cloudfront": cf, "version": version }))
        .send()
        .await
        .map_err(|_| fail(StatusCode::BAD_GATEWAY, "hosted_unreachable", "CoderHelm's servers didn't answer. Check that your install can reach api.coderhelm.com, then try again."))?;
    let code = r.status();
    // The keys this install will hold come in this answer: CoderHelm must
    // have signed it (once the release pins CoderHelm's keys).
    let answer = signed_answer(r, &request_id, "POST", "/hybrid/installs").await?;
    let v: Value = serde_json::from_slice(&answer).unwrap_or_default();
    if !code.is_success() {
        return Err(fail(
            code,
            v["code"].as_str().unwrap_or("register_failed"),
            v["error"]
                .as_str()
                .unwrap_or("CoderHelm refused the registration."),
        ));
    }
    let c = Credentials::from_parts(
        v["install_id"].as_str(),
        v["secret"].as_str(),
        v["relay_key"].as_str(),
    )
    .ok_or_else(|| {
        fail(
            StatusCode::BAD_GATEWAY,
            "register_failed",
            "CoderHelm answered without keys. Try again.",
        )
    })?;
    let mut secret = super::setup::read_secret().await.map_err(|_| {
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "secret_unreadable",
            "CoderHelm can't read its own secret. Open the installer log in AWS.",
        )
    })?;
    secret["hybrid_install_id"] = json!(c.install_id);
    secret["hybrid_install_secret"] = json!(c.secret);
    secret["hybrid_relay_key"] = json!(c.relay_key);
    let revision = super::setup::write_secret(&secret).await.map_err(|_| {
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "secret_unwritable",
            "CoderHelm couldn't save the keys in its secret.",
        )
    })?;
    client::set_credentials(Some(c.clone()));
    // The worker and the other gateway copies read the keys at start.
    super::setup::refresh_services(state, &revision).await;
    l.install_id = c.install_id.clone();
    l.cloudfront = cf;
    l.registered_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    l.verified_at.clear();
    save_local(state, &l).await?;
    info!(install = %c.install_id, "hybrid: registered with CoderHelm");
    let _ = do_verify(state, &mut l).await;
    // The Other apps choice (CoderHelm's apps unless someone chose private).
    l.apps_sent.clear();
    sync_apps(state, &mut l, None).await;
    Ok(l)
}

async fn do_verify(state: &AppState, l: &mut Local) -> Result<(), (StatusCode, Json<Value>)> {
    let c = creds_now().await.ok_or_else(not_registered)?;
    client::call(
        &state.http,
        &c,
        "POST",
        "/hybrid/installs/verify",
        Some(&json!({})),
    )
    .await
    .map_err(from_call)?;
    l.verified_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    save_local(state, l).await
}

fn not_registered() -> (StatusCode, Json<Value>) {
    fail(
        StatusCode::CONFLICT,
        "not_registered",
        "Register this install with CoderHelm first.",
    )
}

async fn ensure_verified(
    state: &AppState,
) -> Result<(Credentials, Local), (StatusCode, Json<Value>)> {
    let mut l = local(state).await;
    let c = creds_now().await.ok_or_else(not_registered)?;
    if l.verified_at.is_empty() {
        do_verify(state, &mut l).await?;
    }
    Ok((c, l))
}

async fn do_claim(
    state: &AppState,
    raw: &str,
    actor: &str,
) -> Result<Local, (StatusCode, Json<Value>)> {
    let name = names::validate_name(raw)
        .map_err(|p| fail(StatusCode::BAD_REQUEST, p.code(), p.message()))?;
    let (c, mut l) = ensure_verified(state).await?;
    client::call(
        &state.http,
        &c,
        "PUT",
        "/hybrid/names",
        Some(&json!({ "name": name })),
    )
    .await
    .map_err(from_call)?;
    l.name = name.clone();
    l.records_sent.clear();
    save_local(state, &l).await?;
    // Ask the updater for the certificate; it finds no zone for
    // coderhelm.com in this account and lists the two records, which
    // `sync_name_records` sends to CoderHelm.
    if super::system::has_updater() {
        let body = super::system::call(state, json!({ "action": "domain_set", "hostname": names::hostname(&name), "actor": actor }))
            .await
            .map_err(|c| fail(c, "domain_set_failed", "The address is yours, but requesting its certificate failed. Press Try again in Settings, System, Domain."))?;
        if body.get("ok").and_then(Value::as_bool) == Some(false) {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "domain_set_failed",
                body["error"]
                    .as_str()
                    .unwrap_or("Requesting the certificate failed."),
            ));
        }
    }
    Ok(l)
}

async fn do_release(state: &AppState, actor: &str) -> Result<Local, (StatusCode, Json<Value>)> {
    let c = creds_now().await.ok_or_else(not_registered)?;
    let mut l = local(state).await;
    client::call(&state.http, &c, "DELETE", "/hybrid/names", None)
        .await
        .map_err(from_call)?;
    if super::system::has_updater() && !l.name.is_empty() {
        let _ =
            super::system::call(state, json!({ "action": "domain_remove", "actor": actor })).await;
    }
    l.name.clear();
    l.records_sent.clear();
    save_local(state, &l).await?;
    Ok(l)
}

/// The records the updater lists for this install's CoderHelm address, as
/// the hosted side takes them. Empty unless the domain is the claimed name.
/// The address record waits until the install answers on the name (the
/// updater's `live`): until then the name shows CoderHelm's "being set up"
/// page instead of a CloudFront error.
pub fn records_for(view: &Value, name: &str) -> Vec<names::NameRecord> {
    if name.is_empty() || view["hostname"].as_str() != Some(names::hostname(name).as_str()) {
        return vec![];
    }
    let attached = view["state"].as_str() == Some("live");
    view["records"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|r| {
                    Some(names::NameRecord {
                        purpose: r["purpose"].as_str()?.to_string(),
                        name: r["name"].as_str()?.trim_end_matches('.').to_string(),
                        rtype: r["type"].as_str().unwrap_or("CNAME").to_string(),
                        value: r["value"].as_str()?.trim_end_matches('.').to_string(),
                    })
                })
                .filter(|r| attached || r.purpose != "address")
                .collect()
        })
        .unwrap_or_default()
}

/// Sends the name's records to CoderHelm when they're new or changed.
/// Called wherever the domain is checked, so it follows the certificate.
pub async fn sync_name_records(state: &AppState, view: Option<&Value>) {
    let l = local(state).await;
    if l.name.is_empty() {
        return;
    }
    let Some(view) = view else { return };
    let recs = records_for(view, &l.name);
    if recs.is_empty() {
        return;
    }
    let key = serde_json::to_string(&recs).unwrap_or_default();
    if key == l.records_sent {
        return;
    }
    let Some(c) = creds_now().await else { return };
    match client::call(
        &state.http,
        &c,
        "PUT",
        "/hybrid/names/records",
        Some(&json!({ "records": recs })),
    )
    .await
    {
        Ok(_) => {
            let mut l = l;
            l.records_sent = key;
            let _ = save_local(state, &l).await;
            info!(name = %l.name, "hybrid: name records sent to CoderHelm");
        }
        Err(e) => {
            let mut d = Delivery::new(
                Direction::Outbound,
                "hybrid",
                "name.records",
                &names::hostname(&l.name),
            );
            d.finish(
                Status::Failed,
                "CoderHelm didn't take the records",
                if e.is_outage() {
                    e.code.as_str()
                } else {
                    "broker_refused"
                },
            );
            d.attempts.push(deliveries::Attempt {
                at: d.created_at.clone(),
                code: e.status,
                duration_ms: 0,
                error: e.message.clone(),
            });
            super::deliveries::save(state, super::deliveries::UNROUTED, &d).await;
            warn!(error = %e, "hybrid: name records not taken");
        }
    }
}

async fn do_link(
    state: &AppState,
    return_url: Option<&str>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    let (c, _) = ensure_verified(state).await?;
    let ret = return_url
        .filter(|u| u.starts_with("https://"))
        .map(str::to_string)
        .unwrap_or_else(|| {
            format!(
                "{}/settings/system/#hybrid",
                common::public_urls::app_url(&state.config.stage)
            )
        });
    client::call(
        &state.http,
        &c,
        "POST",
        "/hybrid/github/link-start",
        Some(&json!({ "return_url": ret })),
    )
    .await
    .map_err(from_call)
}

#[derive(Deserialize, Default)]
pub struct SettingsBody {
    #[serde(default)]
    pub github_mode: Option<String>,
    #[serde(default)]
    pub integrations: Option<Vec<String>>,
    #[serde(default)]
    pub sharing: Option<SharingBody>,
    /// Settings, Integrations, Jira, "Link through CoderHelm" on or off.
    #[serde(default)]
    pub jira_link: Option<bool>,
    /// Other apps, for the whole install: `coderhelm` or `private`.
    #[serde(default)]
    pub apps_mode: Option<String>,
    /// Earlier releases' per-tool choices, `{ "notion": "coderhelm" | "own" }`,
    /// still taken for one release (older dashboards and install guides):
    /// any `coderhelm` means CoderHelm's apps, only `own` means private.
    /// Ignored when `apps_mode` is sent.
    #[serde(default)]
    pub apps: Option<BTreeMap<String, String>>,
}

#[derive(Deserialize, Default, Clone)]
pub struct SharingBody {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub payloads: bool,
}

/// Applies a settings change to the local choices (pure; tested).
pub fn apply_settings(l: &mut Local, b: &SettingsBody) -> Result<(), &'static str> {
    if let Some(m) = &b.github_mode {
        match m.as_str() {
            "own" | "hosted" => l.github_mode = m.clone(),
            _ => return Err("github_mode is own or hosted"),
        }
    }
    if let Some(list) = &b.integrations {
        let allowed: Vec<&str> = integrations::hosted_ids().collect();
        if list.iter().any(|i| !allowed.contains(&i.as_str())) {
            return Err("an integration that can't use CoderHelm's app");
        }
    }
    if let Some(apps) = &b.apps {
        for (id, v) in apps {
            if !integrations::app_choice_ids().any(|x| x == id) {
                return Err("an integration that can't use CoderHelm's app");
            }
            if v != "coderhelm" && v != "own" {
                return Err("each app is coderhelm or own");
            }
        }
    }
    if let Some(m) = &b.apps_mode {
        match m.as_str() {
            "coderhelm" | "private" => l.apps_mode = m.clone(),
            _ => return Err("apps_mode is coderhelm or private"),
        }
    } else if let Some(mode) = legacy_apps_mode(b) {
        l.apps_mode = mode.into();
    }
    if let Some(s) = &b.sharing {
        l.sharing = s.enabled;
        l.share_payloads = s.enabled && s.payloads;
    }
    if let Some(on) = b.jira_link {
        l.jira_link = on;
    }
    Ok(())
}

async fn do_settings(
    state: &AppState,
    b: &SettingsBody,
) -> Result<Local, (StatusCode, Json<Value>)> {
    let mut l = local(state).await;
    let was_linking = l.jira_link;
    migrate_apps(&mut l);
    apply_settings(&mut l, b).map_err(|m| fail(StatusCode::BAD_REQUEST, "bad_settings", m))?;
    if l.jira_link && creds_now().await.is_none() {
        return Err(not_registered());
    }
    if was_linking && !l.jira_link {
        // Off: unlink, so CoderHelm handles the site's events again.
        jira_unlink_now(state, &mut l).await?;
    }
    if let Some(c) = creds_now().await {
        let want = wanted_integrations(&l, |id| own_app_set(state, id));
        let v = client::call(
            &state.http,
            &c,
            "PUT",
            "/hybrid/settings",
            Some(&json!({ "integrations": want, "sharing": { "enabled": l.sharing, "payloads": l.share_payloads } })),
        )
        .await
        .map_err(from_call)?;
        l.integrations = strings(&v["integrations"]).unwrap_or_else(|| want.clone());
        l.apps_sent = want;
    } else if l.github_mode == "hosted" || l.sharing {
        return Err(not_registered());
    }
    save_local(state, &l).await?;
    Ok(l)
}

/// A JSON array of strings, if it is one.
fn strings(v: &Value) -> Option<Vec<String>> {
    v.as_array().map(|a| {
        a.iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })
}

/// Does this install have its own app set up for a tool (its own Notion,
/// Atlassian, Google or Figma client in its secret)? Then the tool uses it,
/// whichever Other apps choice the install made.
fn own_app_set(state: &AppState, id: &str) -> bool {
    common::knowledge::sources::Kind::parse(id)
        .is_some_and(|k| super::knowledge_sources::app(state, k).is_set())
        || super::mcp_servers::coderhelm_app(state, id).is_set()
}

/// The Other apps choice, `coderhelm` unless someone chose `private`.
pub fn apps_mode(l: &Local) -> &'static str {
    if l.apps_mode == "private" {
        "private"
    } else {
        "coderhelm"
    }
}

/// Does `id` use CoderHelm's app: the install chose CoderHelm's apps and has
/// no app of its own for `id`. Pure; tested.
pub fn uses_coderhelm(l: &Local, id: &str, own_app_set: impl Fn(&str) -> bool) -> bool {
    apps_mode(l) == "coderhelm" && !own_app_set(id)
}

/// The install-wide choice an earlier release's settings body means: the
/// per-tool `apps`, or the whole `integrations` list from before them. Any
/// tool on CoderHelm's app means CoderHelm's apps; only `own` (or a list
/// with none of the tools) means private, so an older page never makes an
/// install less private than it asked. `None` when neither is sent.
pub fn legacy_apps_mode(b: &SettingsBody) -> Option<&'static str> {
    let pick = |coderhelm: bool| if coderhelm { "coderhelm" } else { "private" };
    if let Some(apps) = b.apps.as_ref().filter(|a| !a.is_empty()) {
        return Some(pick(apps.values().any(|v| v == "coderhelm")));
    }
    b.integrations
        .as_ref()
        .map(|list| pick(integrations::app_choice_ids().any(|id| list.iter().any(|i| i == id))))
}

/// The integrations to ask CoderHelm for: GitHub when its mode is hosted,
/// then each Other apps tool that uses CoderHelm's app. Pure; tested.
pub fn wanted_integrations(l: &Local, own_app_set: impl Fn(&str) -> bool) -> Vec<String> {
    let mut out = vec![];
    if l.github_mode == "hosted" {
        out.push("github".to_string());
    }
    out.extend(
        integrations::app_choice_ids()
            .filter(|id| uses_coderhelm(l, id, &own_app_set))
            .map(str::to_string),
    );
    out
}

/// Turns an earlier release's per-tool choices into the install-wide one,
/// once. A tool was on CoderHelm's app when someone chose that for it, or
/// when nobody chose and CoderHelm holds it (a record from before per-tool
/// choices kept them only in that list). Any tool on CoderHelm's app means
/// CoderHelm's apps; tools all chosen as their own means private; nothing
/// chosen means the default, CoderHelm's apps. True when the record
/// changed. Pure; tested.
pub fn migrate_apps(l: &mut Local) -> bool {
    if l.apps_v >= 2 {
        return false;
    }
    l.apps_v = 2;
    if !l.apps_mode.is_empty() {
        return true;
    }
    let on = |id: &str| match l.apps.get(id).map(String::as_str) {
        Some(v) => v == "coderhelm",
        None => l.integrations.iter().any(|i| i == id),
    };
    let any_own = l.apps.values().any(|v| v == "own");
    l.apps_mode = if integrations::app_choice_ids().any(on) || !any_own {
        "coderhelm".into()
    } else {
        "private".into()
    };
    true
}

fn same_set(a: &[String], b: &[String]) -> bool {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort();
    b.sort();
    a == b
}

/// Should the wanted list be sent to CoderHelm again: it changed since it
/// was last sent, or CoderHelm holds something else than this install
/// thinks. Pure; tested.
pub fn apps_out_of_date(l: &Local, want: &[String], hosted_now: Option<&[String]>) -> bool {
    !same_set(want, &l.apps_sent) || hosted_now.is_some_and(|h| !same_set(h, &l.integrations))
}

/// Brings the integrations CoderHelm holds in line with the choices (and
/// the defaults for the tools nobody chose). Only which app a sign-in goes
/// through changes: nothing is connected or synced here.
async fn sync_apps(state: &AppState, l: &mut Local, hosted: Option<&Value>) {
    let offered = offered_by(hosted);
    let mut changed = migrate_apps(l);
    let mut want = wanted_integrations(l, |id| own_app_set(state, id));
    if let Some(o) = &offered {
        want.retain(|i| o.contains(i));
    }
    let hosted_now = hosted.and_then(|h| strings(&h["integrations"]));
    if apps_out_of_date(l, &want, hosted_now.as_deref()) {
        if let Some(c) = creds_now().await {
            match client::call(
                &state.http,
                &c,
                "PUT",
                "/hybrid/settings",
                Some(&json!({ "integrations": want, "sharing": { "enabled": l.sharing, "payloads": l.share_payloads } })),
            )
            .await
            {
                Ok(v) => {
                    l.integrations = strings(&v["integrations"]).unwrap_or_else(|| want.clone());
                    l.apps_sent = want;
                    changed = true;
                }
                Err(e) => warn!(code = %e.code, "hybrid: app choices not sent"),
            }
        }
    }
    if changed {
        let _ = save_local(state, l).await;
    }
}

// ── Status and checks ────────────────────────────────────────────────────

/// One hybrid check: id, status, code, plain title and fix.
fn check(id: &str, status: &str, code: &str, title: &str, fix: &str) -> Value {
    json!({ "id": id, "status": status, "code": code, "title": title, "fix": if fix.is_empty() { Value::Null } else { json!(fix) } })
}

static LAST_INTAKE_PROBLEM: Mutex<Option<(Instant, &'static str)>> = Mutex::new(None);

/// A relay signature problem from the last hour, if any.
fn recent_intake_problem() -> Option<&'static str> {
    LAST_INTAKE_PROBLEM
        .lock()
        .ok()
        .and_then(|g| *g)
        .filter(|(at, _)| at.elapsed() < Duration::from_secs(3600))
        .map(|(_, p)| p)
}

/// The checks for Settings and the guide, from what's known (pure; tested).
pub fn checks(
    l: &Local,
    registered: bool,
    hosted: Option<&Value>,
    domain_code: Option<&str>,
    intake_problem: Option<&str>,
) -> Vec<Value> {
    let mut out = vec![];
    if !registered {
        out.push(check(
            "hybrid_registered",
            "todo",
            "not_registered",
            "Not registered with CoderHelm",
            "Press Register: it takes a few seconds and changes nothing in AWS.",
        ));
        return out;
    }
    let verified = hosted
        .map(|h| h["verified"].as_bool() == Some(true))
        .unwrap_or(!l.verified_at.is_empty());
    out.push(if verified {
        check("hybrid_registered", "pass", "ok", "Registered and verified", "")
    } else {
        check("hybrid_registered", "fail", "proof_failed", "CoderHelm can't reach your install yet", "It restarts after registering. Wait a minute and press Check again; if it stays, check that its CloudFront address opens.")
    });
    if hosted.is_some_and(|h| h["status"].as_str() == Some("revoked")) {
        out.push(check(
            "hybrid_access",
            "fail",
            "broker_revoked",
            "CoderHelm turned off this install's access",
            "Contact support, or switch to your own GitHub App and domain.",
        ));
    }
    if !l.name.is_empty() {
        let host = names::hostname(&l.name);
        out.push(match domain_code {
            Some("ok") => check("hybrid_name", "pass", "ok", &format!("{host} is live"), ""),
            Some("validating") | Some("dns_missing") => check("hybrid_name", "wait", "cert_pending", &format!("Setting up {host}"), "CoderHelm added the records. AWS issues the certificate within about 30 minutes, then CoderHelm moves to the address by itself."),
            Some("attaching") => check("hybrid_name", "wait", "attaching", &format!("Moving to {host}"), "About 10 minutes. CoderHelm keeps working on its AWS address meanwhile."),
            Some(c) if c != "none" => check("hybrid_name", "fail", c, &format!("{host} isn't working yet"), "Open Settings, System, Domain for the reason and press Try again."),
            _ => check("hybrid_name", "wait", "requested", &format!("Requesting the certificate for {host}"), "This page checks again by itself."),
        });
    }
    if l.github_mode == "hosted" {
        let links = hosted
            .and_then(|h| h["github"]["links"].as_array())
            .map(Vec::len)
            .unwrap_or(0);
        out.push(if links > 0 {
            check(
                "hybrid_github",
                "pass",
                "ok",
                "CoderHelm's GitHub App is linked",
                "",
            )
        } else {
            check(
                "hybrid_github",
                "todo",
                "no_link",
                "Install CoderHelm's GitHub App on your organization",
                "Press Install on GitHub, pick the repositories, and you come back here linked.",
            )
        });
    }
    if l.jira_link {
        let link = hosted.map(|h| &h["jira"]["link"]).filter(|v| v.is_object());
        out.push(match link {
            Some(v) => {
                let site = v["site_url"].as_str().filter(|s| !s.is_empty()).unwrap_or("a Jira webhook");
                if l.jira_synced_at.is_empty() || v["linked_at"].as_str() != Some(l.jira_synced_at.as_str()) {
                    check("hybrid_jira", "wait", "syncing", &format!("Linking {site}"), "CoderHelm linked the site. This install copies its Jira app settings the next time this page checks.")
                } else {
                    check("hybrid_jira", "pass", "ok", &format!("{site} is linked through CoderHelm"), "")
                }
            }
            None => check(
                "hybrid_jira",
                "todo",
                "no_link",
                "Link your Jira site through CoderHelm",
                "Open Settings, Integrations, Jira and press Link: sign in to CoderHelm as an admin of the team that connected the site, and confirm.",
            ),
        });
    }
    if let Some(p) = intake_problem {
        let sp = match p {
            "signature_mismatch" => sign::SignatureProblem::Mismatch,
            "signature_stale" => sign::SignatureProblem::Stale,
            _ => sign::SignatureProblem::Malformed,
        };
        out.push(check("hybrid_relay", "fail", p, sp.message(), sp.fix()));
    }
    out
}

/// The hosted side's `offered` list; `None` from a hosted side that predates
/// it (then everything hosted counts as offered).
pub fn offered_by(hosted: Option<&Value>) -> Option<Vec<String>> {
    hosted?["offered"].as_array().map(|a| {
        a.iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })
}

async fn status_value(state: &AppState, deep: bool) -> Value {
    let mut l = local(state).await;
    let creds = creds_now().await;
    let hosted = match &creds {
        Some(c) => client::call(&state.http, c, "GET", "/hybrid/status", None)
            .await
            .ok(),
        None => None,
    };
    if let (Some(_), Some(h)) = (&creds, &hosted) {
        if h["verified"].as_bool() != Some(true) && deep {
            let _ = do_verify(state, &mut l).await;
        }
    }
    let (facts, view) = super::setup_checks::domain_facts_now(state).await;
    sync_name_records(state, view.as_ref()).await;
    let (_, code) = super::setup_checks::domain_code(&facts);
    if let Some(h) = &hosted {
        let _ = sync_jira(state, &mut l, Some(h)).await;
    }
    let problem = recent_intake_problem();
    let mut list = checks(&l, creds.is_some(), hosted.as_ref(), Some(code), problem);
    // CoderHelm's 15-minute name check parked the address (it pointed
    // somewhere that no longer answered as this install, or a record under it
    // wasn't CoderHelm's): say so with the reason, where the domain is set up.
    let name_check = hosted
        .as_ref()
        .map(|h| h["name_check"].clone())
        .unwrap_or(Value::Null);
    if !l.name.is_empty() && name_check["parked"].as_bool() == Some(true) {
        list.push(check(
            "hybrid_name_parked",
            "fail",
            name_check["parked_code"].as_str().unwrap_or("parked"),
            name_check["parked_reason"].as_str().unwrap_or("CoderHelm put this address back on its holding page."),
            "Open Settings, System, Domain and press Try again. If the install moved to a new CloudFront address, set the name up again there.",
        ));
    }
    if deep && l.github_mode == "hosted" {
        if let Some(first) = hosted
            .as_ref()
            .and_then(|h| h["github"]["links"][0]["installation_id"].as_u64())
        {
            // A person checking now gets the broker's answer now, not the
            // wait a background sweep is in.
            client::forget_token_backoff(first);
            list.push(match client::github_token(&state.http, first).await {
                Ok(_) => check(
                    "hybrid_broker",
                    "pass",
                    "ok",
                    "Tokens from CoderHelm's broker work",
                    "",
                ),
                Err(e) => {
                    let r = deliveries::reason(&e.code);
                    check("hybrid_broker", "fail", &e.code, r.message, r.fix)
                }
            });
        }
    }
    // What the hosted side can serve now: a choice it no longer offers (its
    // app for that provider isn't set up) is dropped, so the add flow never
    // offers a sign-in the broker would refuse.
    let offered = offered_by(hosted.as_ref());
    if creds.is_some() {
        sync_apps(state, &mut l, hosted.as_ref()).await;
    } else if migrate_apps(&mut l) {
        let _ = save_local(state, &l).await;
    }
    let chosen: Vec<Value> = integrations::CATALOG
        .iter()
        .map(|e| {
            let mut v = serde_json::to_value(e).unwrap_or_default();
            // Registered: what CoderHelm holds. Not yet: what registering applies.
            let uses = if creds.is_some() {
                l.integrations.iter().any(|i| i == e.id)
            } else {
                wanted_integrations(&l, |id| own_app_set(state, id))
                    .iter()
                    .any(|i| i == e.id)
            };
            v["uses_coderhelm"] = json!(uses);
            // The install's own app for this tool, which it uses whichever
            // Other apps choice was made.
            if integrations::app_choice_ids().any(|id| id == e.id) {
                v["own_app"] = json!(own_app_set(state, e.id));
            }
            v["offered"] = json!(offered.as_ref().is_none_or(|o| o.iter().any(|i| i == e.id)));
            v
        })
        .collect();
    json!({
        "available": super::setup_checks::self_hosted(),
        "hosted_url": common::hybrid::hosted_url(),
        // CoderHelm's public keys this release pins (Settings, System, Hybrid
        // shows them to compare with the fingerprints CoderHelm publishes).
        "coderhelm_keys": coderhelm_keys(),
        "registered": creds.is_some(),
        "install_id": creds.as_ref().map(|c| c.install_id.clone()),
        "cloudfront": l.cloudfront,
        "verified": hosted.as_ref().map(|h| h["verified"].as_bool() == Some(true)).unwrap_or(false),
        "name": if l.name.is_empty() { Value::Null } else { json!({ "name": l.name, "hostname": names::hostname(&l.name), "code": code, "check": name_check }) },
        "github": {
            "mode": if l.github_mode.is_empty() { "own" } else { l.github_mode.as_str() },
            "links": hosted.as_ref().map(|h| h["github"]["links"].clone()).unwrap_or(json!([])),
            "app_slug": hosted.as_ref().map(|h| h["github"]["app_slug"].clone()).unwrap_or(Value::Null),
        },
        "apps_mode": apps_mode(&l),
        "integrations": chosen,
        "sharing": { "enabled": l.sharing, "payloads": l.share_payloads },
        "jira": {
            "enabled": l.jira_link,
            "link": hosted.as_ref().map(|h| h["jira"]["link"].clone()).unwrap_or(Value::Null),
            "synced": !l.jira_synced_at.is_empty(),
        },
        "checks": list,
    })
}

// ── Settings routes (install admins) ─────────────────────────────────────

/// GET /api/system/hybrid?deep=1
pub async fn get_status(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<DeepQuery>,
) -> Reply {
    admin(&claims)?;
    Ok(Json(status_value(&state, q.deep.is_some()).await))
}

#[derive(Deserialize, Default)]
pub struct DeepQuery {
    deep: Option<String>,
}

/// POST /api/system/hybrid/register
pub async fn register(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    admin(&claims)?;
    do_register(&state).await?;
    Ok(Json(status_value(&state, false).await))
}

/// DELETE /api/system/hybrid: leave hybrid (release the name, unlink,
/// revoke the sign-ins) and forget the registration.
pub async fn unregister(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    admin(&claims)?;
    if let Some(c) = creds_now().await {
        client::call(&state.http, &c, "DELETE", "/hybrid/installs", None)
            .await
            .map_err(from_call)?;
    }
    if let Ok(mut secret) = super::setup::read_secret().await {
        if let Some(o) = secret.as_object_mut() {
            o.remove("hybrid_install_id");
            o.remove("hybrid_install_secret");
            o.remove("hybrid_relay_key");
        }
        if let Ok(rev) = super::setup::write_secret(&secret).await {
            super::setup::refresh_services(&state, &rev).await;
        }
    }
    client::set_credentials(None);
    // The linked Jira site's settings go, and the install's own come back.
    let mut l = local(&state).await;
    forget_jira(&state, &mut l).await;
    save_local(&state, &Local::default()).await?;
    Ok(Json(status_value(&state, false).await))
}

/// GET /api/system/hybrid/names/:name — is it free? (CoderHelm's public check)
pub async fn name_available(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Reply {
    admin(&claims)?;
    available(&state, &name).await
}

async fn available(state: &AppState, raw: &str) -> Reply {
    if let Err(p) = names::validate_name(raw) {
        return Ok(Json(
            json!({ "name": raw, "available": false, "code": p.code(), "error": p.message() }),
        ));
    }
    let url = format!(
        "{}/hybrid/names/{}",
        common::hybrid::hosted_url(),
        urlencoding::encode(raw.trim())
    );
    let request_id = hosted_sig::new_request_id();
    let path = format!("/hybrid/names/{}", urlencoding::encode(raw.trim()));
    let r = state
        .http
        .get(url)
        .timeout(Duration::from_secs(6))
        .header(hosted_sig::REQUEST_ID_HEADER, &request_id)
        .send()
        .await
        .map_err(|_| {
            fail(
                StatusCode::BAD_GATEWAY,
                "hosted_unreachable",
                "CoderHelm's servers didn't answer.",
            )
        })?;
    let answer = signed_answer(r, &request_id, "GET", &path).await?;
    Ok(Json(serde_json::from_slice(&answer).unwrap_or_default()))
}

#[derive(Deserialize, Default)]
pub struct NameBody {
    #[serde(default)]
    name: String,
}

/// PUT /api/system/hybrid/name `{name}`
pub async fn claim_name(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(b): Json<NameBody>,
) -> Reply {
    admin(&claims)?;
    do_claim(&state, &b.name, &claims.email).await?;
    Ok(Json(status_value(&state, false).await))
}

/// DELETE /api/system/hybrid/name
pub async fn release_name(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    admin(&claims)?;
    do_release(&state, &claims.email).await?;
    Ok(Json(status_value(&state, false).await))
}

#[derive(Deserialize, Default)]
pub struct LinkBody {
    #[serde(default)]
    return_url: Option<String>,
    /// Jira: the site the install expects (CoderHelm links only that one).
    #[serde(default)]
    site_url: Option<String>,
}

/// POST /api/system/hybrid/github/link — where to install CoderHelm's GitHub App.
pub async fn github_link(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    body: Option<Json<LinkBody>>,
) -> Reply {
    admin(&claims)?;
    let b = body.map(|Json(b)| b).unwrap_or_default();
    Ok(Json(do_link(&state, b.return_url.as_deref()).await?))
}

/// PUT /api/system/hybrid/github/:installation_id `{repositories}`
pub async fn github_repositories(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(iid): Path<u64>,
    Json(b): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let c = creds_now().await.ok_or_else(not_registered)?;
    client::call(
        &state.http,
        &c,
        "PUT",
        &format!("/hybrid/github/links/{iid}"),
        Some(&json!({ "repositories": b["repositories"] })),
    )
    .await
    .map_err(from_call)?;
    Ok(Json(status_value(&state, false).await))
}

/// DELETE /api/system/hybrid/github/:installation_id
pub async fn github_unlink(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(iid): Path<u64>,
) -> Reply {
    admin(&claims)?;
    let c = creds_now().await.ok_or_else(not_registered)?;
    client::call(
        &state.http,
        &c,
        "DELETE",
        &format!("/hybrid/github/links/{iid}"),
        None,
    )
    .await
    .map_err(from_call)?;
    Ok(Json(status_value(&state, false).await))
}

/// POST /api/system/hybrid/jira/link `{return_url?}` — the CoderHelm page
/// where an admin of the team that connected the Jira site confirms the
/// link. Needs Settings, Integrations, Jira, "Link through CoderHelm" on.
pub async fn jira_link(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    body: Option<Json<LinkBody>>,
) -> Reply {
    admin(&claims)?;
    let l = local(&state).await;
    if !l.jira_link {
        return Err(fail(
            StatusCode::CONFLICT,
            "jira_link_off",
            "Turn on Link through CoderHelm first.",
        ));
    }
    let b = body.map(|Json(b)| b).unwrap_or_default();
    let site = b
        .site_url
        .as_deref()
        .and_then(super::hybrid_jira::site_host)
        .ok_or_else(|| {
            fail(
                StatusCode::BAD_REQUEST,
                "bad_site",
                "Type your Jira site's address, e.g. example.atlassian.net.",
            )
        })?;
    let (c, _) = ensure_verified(&state).await?;
    let ret = b
        .return_url
        .filter(|u| u.starts_with("https://"))
        .unwrap_or_else(|| {
            format!(
                "{}/settings/integrations/jira/",
                common::public_urls::app_url(&state.config.stage)
            )
        });
    client::call(
        &state.http,
        &c,
        "POST",
        "/hybrid/jira/link-start",
        Some(&json!({ "return_url": ret, "site_url": site })),
    )
    .await
    .map(Json)
    .map_err(from_call)
}

/// The Jira settings a link replaces on the install's team: the site, the
/// app's secret and its links (here a token and CoderHelm's proxy links).
pub fn jira_fields() -> Vec<&'static str> {
    let mut f = vec!["site_url", "forge_secret"];
    f.extend(super::hybrid_jira::TRIGGERS.iter().map(|(field, _)| *field));
    f
}

/// Whether the install's Jira settings are its own (not a link's copy): a
/// link's links all go through CoderHelm's proxy.
pub fn own_jira_settings(item: &std::collections::HashMap<String, AttributeValue>) -> bool {
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    !s("forge_secret").is_empty() && !s("add_comment_url").contains("/hybrid/jira/trigger/")
}

async fn team_jira_item(
    state: &AppState,
    team: &str,
) -> Option<std::collections::HashMap<String, AttributeValue>> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", AttributeValue::S(team.into()))
        .key("sk", AttributeValue::S("JIRA#config".into()))
        .consistent_read(true)
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
}

/// DELETE /api/system/hybrid/jira/link — unlink the site (CoderHelm handles
/// its events again) and forget the copied Jira app settings.
pub async fn jira_unlink(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    admin(&claims)?;
    let mut l = local(&state).await;
    jira_unlink_now(&state, &mut l).await?;
    save_local(&state, &l).await?;
    Ok(Json(status_value(&state, false).await))
}

async fn jira_unlink_now(state: &AppState, l: &mut Local) -> Result<(), (StatusCode, Json<Value>)> {
    if let Some(c) = creds_now().await {
        client::call(&state.http, &c, "DELETE", "/hybrid/jira/link", None)
            .await
            .map_err(from_call)?;
    }
    forget_jira(state, l).await;
    Ok(())
}

/// What a sync should do, from the link CoderHelm reports and what this
/// install last copied (pure; tested).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JiraSync {
    Nothing,
    /// Copy the linked site's settings (a new link, or a relink).
    Copy,
    /// The link is gone on CoderHelm's side: forget the copy.
    Forget,
}

pub fn jira_sync_plan(l: &Local, link: Option<&Value>) -> JiraSync {
    match link.filter(|v| v.is_object()) {
        Some(v) if l.jira_link => {
            if v["linked_at"].as_str() == Some(l.jira_synced_at.as_str())
                && !l.jira_synced_at.is_empty()
            {
                JiraSync::Nothing
            } else {
                JiraSync::Copy
            }
        }
        _ if !l.jira_synced_at.is_empty() => JiraSync::Forget,
        _ => JiraSync::Nothing,
    }
}

/// Keeps this install's Jira settings in step with the link on CoderHelm:
/// copies the linked site's Jira app settings to the install's team (so it
/// checks the Forge secret on each event and can comment back), or forgets
/// them once the link is gone. `hosted` is CoderHelm's status answer.
pub async fn sync_jira(
    state: &AppState,
    l: &mut Local,
    hosted: Option<&Value>,
) -> Result<(), &'static str> {
    let link = hosted.map(|h| &h["jira"]["link"]);
    match jira_sync_plan(l, link) {
        JiraSync::Nothing => Ok(()),
        JiraSync::Forget => {
            forget_jira(state, l).await;
            let _ = save_local(state, l).await;
            Ok(())
        }
        JiraSync::Copy => {
            let c = creds_now().await.ok_or("jira_link_unsynced")?;
            let team = only_team(state).await.ok_or("no_team")?;
            let cfg = client::call(
                &state.http,
                &c,
                "POST",
                "/hybrid/jira/config",
                Some(&json!({})),
            )
            .await
            .map_err(|_| "jira_link_unsynced")?;
            // The install's own Jira app settings, kept aside the first time
            // a link replaces them, and put back when it is unlinked.
            let existing = team_jira_item(state, &team).await.unwrap_or_default();
            let keep = own_jira_settings(&existing) && !existing.contains_key("own_forge_secret");
            let mut expr = vec!["updated_at = :at".to_string()];
            let mut req = state
                .dynamo
                .update_item()
                .table_name(&state.config.jira_config_table_name)
                .key("pk", AttributeValue::S(team))
                .key("sk", AttributeValue::S("JIRA#config".into()))
                .expression_attribute_values(
                    ":at",
                    AttributeValue::S(chrono::Utc::now().to_rfc3339()),
                );
            for (n, k) in jira_fields().into_iter().enumerate() {
                expr.push(format!("#f{n} = :f{n}"));
                req = req
                    .expression_attribute_names(format!("#f{n}"), k)
                    .expression_attribute_values(
                        format!(":f{n}"),
                        AttributeValue::S(cfg[k].as_str().unwrap_or("").into()),
                    );
                if keep {
                    if let Some(v) = existing.get(k) {
                        expr.push(format!("#o{n} = :o{n}"));
                        req = req
                            .expression_attribute_names(format!("#o{n}"), format!("own_{k}"))
                            .expression_attribute_values(format!(":o{n}"), v.clone());
                    }
                }
            }
            req.update_expression(format!("SET {}", expr.join(", ")))
                .send()
                .await
                .map_err(|_| "jira_link_unsynced")?;
            let v = link.cloned().unwrap_or_default();
            l.jira_site = v["site_url"].as_str().unwrap_or("").to_string();
            l.jira_synced_at = v["linked_at"].as_str().unwrap_or("").to_string();
            save_local(state, l)
                .await
                .map_err(|_| "jira_link_unsynced")?;
            info!(site = %l.jira_site, "hybrid: copied the linked Jira site's settings");
            Ok(())
        }
    }
}

/// Forgets the copied Jira app settings (the Forge secret and links) once
/// the site is unlinked, so nothing here acts for it any more.
async fn forget_jira(state: &AppState, l: &mut Local) {
    if l.jira_synced_at.is_empty() {
        return;
    }
    if let Some(team) = only_team(state).await {
        let existing = team_jira_item(state, &team).await.unwrap_or_default();
        let mut sets = vec![];
        let mut removes = vec![];
        let mut req = state
            .dynamo
            .update_item()
            .table_name(&state.config.jira_config_table_name)
            .key("pk", AttributeValue::S(team))
            .key("sk", AttributeValue::S("JIRA#config".into()));
        for (n, k) in jira_fields().into_iter().enumerate() {
            req = req.expression_attribute_names(format!("#f{n}"), k);
            match existing.get(&format!("own_{k}")) {
                // The install's own setting comes back.
                Some(v) => {
                    req = req
                        .expression_attribute_names(format!("#o{n}"), format!("own_{k}"))
                        .expression_attribute_values(format!(":o{n}"), v.clone());
                    sets.push(format!("#f{n} = :o{n}"));
                    removes.push(format!("#o{n}"));
                }
                None => removes.push(format!("#f{n}")),
            }
        }
        let mut update = String::new();
        if !sets.is_empty() {
            update.push_str(&format!("SET {} ", sets.join(", ")));
        }
        update.push_str(&format!("REMOVE {}", removes.join(", ")));
        let _ = req
            .update_expression(update)
            .condition_expression("attribute_exists(pk)")
            .send()
            .await;
    }
    l.jira_site.clear();
    l.jira_synced_at.clear();
}

/// The linked site's settings are copied here, fetching them now when they
/// aren't yet (the first event after linking can beat the settings page).
async fn ensure_jira_synced(state: &AppState) -> Result<(), (StatusCode, &'static str)> {
    let mut l = local(state).await;
    if !l.jira_link {
        return Err((StatusCode::NOT_FOUND, "jira_not_linked"));
    }
    if !l.jira_synced_at.is_empty() {
        return Ok(());
    }
    let c = creds_now()
        .await
        .ok_or((StatusCode::NOT_FOUND, "jira_not_linked"))?;
    let hosted = client::call(&state.http, &c, "GET", "/hybrid/status", None)
        .await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "jira_link_unsynced"))?;
    if !hosted["jira"]["link"].is_object() {
        return Err((StatusCode::NOT_FOUND, "jira_not_linked"));
    }
    sync_jira(state, &mut l, Some(&hosted))
        .await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "jira_link_unsynced"))
}

/// PUT /api/system/hybrid/settings `{github_mode?, sharing?, jira_link?, apps_mode?}` (and, for one release, the older `integrations?` and `apps?`)
pub async fn put_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(b): Json<SettingsBody>,
) -> Reply {
    admin(&claims)?;
    do_settings(&state, &b).await?;
    Ok(Json(status_value(&state, false).await))
}

// ── Guide routes (setup key, until the first sign-up) ────────────────────
//
// The install guide (docs.coderhelm.com) calls these from the browser. Each
// is a simple cross-origin POST (a text/plain JSON body carrying the setup
// key, no custom headers), so there is no preflight; the answer allows any
// origin, since the setup key in the body is the only credential.

#[derive(Deserialize, Default)]
pub struct GuideBody {
    #[serde(default)]
    setup_token: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    return_url: Option<String>,
    #[serde(default)]
    github_mode: Option<String>,
    #[serde(default)]
    integrations: Option<Vec<String>>,
    #[serde(default)]
    sharing: Option<SharingBody>,
    /// The guide's "Use CoderHelm's Jira app": turns on Settings,
    /// Integrations, Jira, "Link through CoderHelm" (linking itself needs an
    /// install admin, after sign-up).
    #[serde(default)]
    jira_link: Option<bool>,
    /// The guide's Other apps choice: `coderhelm` or `private`.
    #[serde(default)]
    apps_mode: Option<String>,
    /// An earlier guide's per-tool choices (see [`SettingsBody::apps`]).
    #[serde(default)]
    apps: Option<BTreeMap<String, String>>,
}

fn guide_body(body: &[u8]) -> GuideBody {
    serde_json::from_slice(body).unwrap_or_default()
}

/// Any origin may read a guide answer (no cookies are involved).
fn guide_reply(r: Reply) -> axum::response::Response {
    use axum::response::IntoResponse;
    let cors = [(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")];
    match r {
        Ok(v) => (cors, v).into_response(),
        Err((code, v)) => (code, cors, v).into_response(),
    }
}

async fn guide_key(state: &AppState, b: &GuideBody) -> Result<(), (StatusCode, Json<Value>)> {
    let mut h = HeaderMap::new();
    if let Ok(v) = b.setup_token.parse() {
        h.insert("x-coderhelm-setup-token", v);
    }
    setup_key(state, &h).await
}

/// POST /api/setup/hybrid `{setup_token}`: the hybrid checks for the guide.
pub async fn setup_status(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> axum::response::Response {
    let b = guide_body(&body);
    let r = async {
        guide_key(&state, &b).await?;
        Ok(Json(status_value(&state, true).await))
    }
    .await;
    guide_reply(r)
}

/// POST /api/setup/hybrid/register `{setup_token}`
pub async fn setup_register(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> axum::response::Response {
    let b = guide_body(&body);
    let r = async {
        guide_key(&state, &b).await?;
        do_register(&state).await?;
        Ok(Json(status_value(&state, false).await))
    }
    .await;
    guide_reply(r)
}

/// POST /api/setup/hybrid/name `{setup_token, name}`
pub async fn setup_claim_name(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> axum::response::Response {
    let b = guide_body(&body);
    let r = async {
        guide_key(&state, &b).await?;
        do_claim(&state, &b.name, "install guide").await?;
        Ok(Json(status_value(&state, false).await))
    }
    .await;
    guide_reply(r)
}

/// POST /api/setup/hybrid/github/link `{setup_token, return_url}`
pub async fn setup_github_link(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> axum::response::Response {
    let b = guide_body(&body);
    let r = async {
        guide_key(&state, &b).await?;
        Ok(Json(do_link(&state, b.return_url.as_deref()).await?))
    }
    .await;
    guide_reply(r)
}

/// POST /api/setup/hybrid/settings `{setup_token, github_mode?, sharing?, jira_link?, apps_mode?}` (and, for one release, the older `integrations?` and `apps?`)
pub async fn setup_settings(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> axum::response::Response {
    let b = guide_body(&body);
    let r = async {
        guide_key(&state, &b).await?;
        let s = SettingsBody {
            github_mode: b.github_mode.clone(),
            integrations: b.integrations.clone(),
            sharing: b.sharing.clone(),
            jira_link: b.jira_link,
            apps_mode: b.apps_mode.clone(),
            apps: b.apps.clone(),
        };
        do_settings(&state, &s).await?;
        Ok(Json(status_value(&state, false).await))
    }
    .await;
    guide_reply(r)
}

// ── The relay intake ─────────────────────────────────────────────────────

fn note_intake_problem(p: &'static str) {
    if let Ok(mut g) = LAST_INTAKE_PROBLEM.lock() {
        *g = Some((Instant::now(), p));
    }
}

/// POST /webhooks/relay: an event CoderHelm's relay forwarded.
/// CoderHelm's own signature on a relayed event, once this release pins
/// CoderHelm's keys (until then the relay key's signature is the check). It
/// must name this install and carry the relay signature's delivery id and
/// time, so the two can't be mixed from different events.
pub(crate) fn hosted_signed(
    headers: &HeaderMap,
    install_id: &str,
    source: &str,
    event: &str,
    body: &[u8],
    stamp: &sign::RelayStamp,
    now: i64,
) -> Result<(), sign::SignatureProblem> {
    hosted_signed_with(
        hosted_sig::trusted(),
        headers,
        install_id,
        source,
        event,
        body,
        stamp,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn hosted_signed_with(
    keys: &hosted_sig::TrustedKeys,
    headers: &HeaderMap,
    install_id: &str,
    source: &str,
    event: &str,
    body: &[u8],
    stamp: &sign::RelayStamp,
    now: i64,
) -> Result<(), sign::SignatureProblem> {
    if keys.is_empty() {
        return Ok(());
    }
    let h = keys.verify(
        headers
            .get(hosted_sig::HOSTED_HEADER)
            .and_then(|v| v.to_str().ok()),
        install_id,
        source,
        event,
        body,
        now,
    )?;
    if h.delivery_id != stamp.delivery_id || h.ts != stamp.ts {
        return Err(sign::SignatureProblem::Mismatch);
    }
    Ok(())
}

#[utoipa::path(
    post,
    path = "/webhooks/relay",
    tag = "Webhooks",
    operation_id = "receiveRelayedEvent",
    summary = "Events from CoderHelm's relay",
    description = "On a hybrid self-hosted install, where CoderHelm's hosted relay forwards the events \
        of CoderHelm's GitHub App (and Jira app) after checking the provider's own signature. Each \
        request is signed with the install's relay key: `X-CoderHelm-Relay-Signature: t=<unix \
        seconds>,d=<delivery id>,v1=<hex>,v2=<hex>`, where `v1` is the HMAC-SHA256 of \
        `<t>.<d>.<raw body>` and `v2` of `v2.<t>.<d>.<source>.<event>.<raw body>` (so the two \
        headers below are signed too). Both must match: a request without `v2` is refused. \
        Requests older than 5 minutes are refused, and so is a \
        delivery id the install has already taken (`409`). \
        `X-CoderHelm-Relay-Source` is `github` or `jira`; for GitHub, `X-CoderHelm-Relay-Event` is \
        GitHub's event type and the body is GitHub's payload, unchanged.",
    params(
        ("X-CoderHelm-Relay-Signature" = String, Header, description = "`t=<unix seconds>,d=<delivery id>,v1=<hex HMAC-SHA256>`.", example = "t=1761436800,d=72d3162e-cc78-11e3-81ab-4c9367dc0958,v1=5d1a…"),
        ("X-CoderHelm-Relay-Source" = String, Header, description = "Where the event came from: `github` or `jira`.", example = "github"),
        ("X-CoderHelm-Relay-Event" = Option<String>, Header, description = "The provider's event type.", example = "pull_request"),
    ),
    request_body(content = Object, description = "The provider's payload, unchanged.", content_type = "application/json"),
    responses(
        (status = 200, description = "Handled (or acknowledged and ignored)."),
        (status = 401, description = "The signature is missing, wrong, or more than 5 minutes old."),
        (status = 404, description = "This install isn't registered for the relay."),
        (status = 409, description = "This delivery id was already received; the copy was refused."),
    ),
    security(("relay_signature" = [])),
)]
pub async fn intake(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    let Some(c) = creds_now().await else {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "not_registered",
            "This install isn't registered for CoderHelm's relay.",
        ));
    };
    let h = |k: &str| {
        headers
            .get(k)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    };
    let source = h(sign::RELAY_SOURCE_HEADER);
    let event = h(sign::RELAY_EVENT_HEADER);
    let now = chrono::Utc::now().timestamp();
    let checked = sign::verify_relay(
        &c.relay_key,
        headers
            .get(sign::RELAY_HEADER)
            .and_then(|v| v.to_str().ok()),
        &source,
        &event,
        &body,
        now,
    )
    .map_err(|p| (p, "Refused: bad signature"))
    .and_then(|stamp| {
        hosted_signed(&headers, &c.install_id, &source, &event, &body, &stamp, now)
            .map_err(|p| (p, "Refused: not signed by CoderHelm"))?;
        Ok(stamp)
    });
    let stamp = match checked {
        Ok(s) => s,
        Err((p, summary)) => {
            note_intake_problem(p.code());
            let mut d = Delivery::new(
                Direction::Inbound,
                "relay",
                &format!("{source}.{event}"),
                "",
            )
            .with_headers(
                headers
                    .iter()
                    .filter_map(|(k, v)| Some((k.as_str(), v.to_str().ok()?))),
            );
            d.finish(Status::Failed, summary, p.code());
            super::deliveries::save(&state, super::deliveries::UNROUTED, &d).await;
            return Err(fail(StatusCode::UNAUTHORIZED, p.code(), p.message()));
        }
    };
    // Each delivery id once (claims outlive the 5-minute window).
    let claim = common::claim::claim(
        &state.dynamo,
        &state.config.settings_table_name,
        "SYSTEM",
        &sign::replay_key(&stamp.delivery_id),
        sign::REPLAY_TTL_SECS as u64,
    )
    .await;
    match intake_claim_answer(&claim) {
        None => {}
        Some((code, p)) => return Err(fail(code, p, intake_claim_message(p))),
    }
    let handled: Result<StatusCode, (StatusCode, Json<Value>)> = async {
        match source.as_str() {
            "github" => {
                let payload: Value = serde_json::from_slice(&body).map_err(|_| {
                    fail(
                        StatusCode::BAD_REQUEST,
                        "bad_payload",
                        "The body isn't JSON.",
                    )
                })?;
                let installation_id = payload["installation"]["id"].as_u64().ok_or_else(|| {
                    fail(
                        StatusCode::BAD_REQUEST,
                        "bad_payload",
                        "The body names no installation.",
                    )
                })?;
                let mut gh = headers.clone();
                gh.insert(
                    "x-github-event",
                    event
                        .parse()
                        .unwrap_or(axum::http::HeaderValue::from_static("unknown")),
                );
                gh.insert(
                    "x-github-delivery",
                    stamp
                        .delivery_id
                        .parse()
                        .unwrap_or(axum::http::HeaderValue::from_static("")),
                );
                let mut d = super::github_webhook::inbound_delivery(&gh, &body, &event);
                // The redelivery for a relayed event is the relay's own record.
                d.redeliver = None;
                super::github_webhook::process(&state, &event, &payload, installation_id, &mut d)
                    .await
                    .map_err(|code| fail(code, "handler_error", "Handling the event failed."))
            }
            "jira" => {
                let mut payload: Value = serde_json::from_slice(&body).map_err(|_| {
                    fail(
                        StatusCode::BAD_REQUEST,
                        "bad_payload",
                        "The body isn't JSON.",
                    )
                })?;
                if event == "register" {
                    if payload["team_id"].as_str().is_some_and(sign::is_install_id) {
                        if let Some(o) = payload.as_object_mut() {
                            o.remove("team_id");
                        }
                    }
                    let team = only_team(&state).await.ok_or_else(|| {
                        fail(
                            StatusCode::CONFLICT,
                            "no_team",
                            "This install has no team yet. Sign up first.",
                        )
                    })?;
                    payload["team_id"] = json!(team);
                    return super::api::forge_register_urls(State(state.clone()), Json(payload))
                        .await
                        .map_err(|code| {
                            fail(
                                code,
                                "handler_error",
                                "Registering the Jira app's links failed.",
                            )
                        });
                }
                // A hosted team's Jira site this install linked through CoderHelm:
                // CoderHelm checked the event (the site's Forge secret or the
                // webhook's signature) and signed the forward; the secret itself
                // never reaches this install. Refused unless the link is on here.
                if event == "site" || event == "site_webhook" {
                    if let Err((code, reason)) = ensure_jira_synced(&state).await {
                        let msg = deliveries::reason(reason).message;
                        return Err(fail(code, reason, msg));
                    }
                    let team = only_team(&state).await.ok_or_else(|| {
                        fail(
                            StatusCode::CONFLICT,
                            "no_team",
                            "This install has no team yet. Sign up first.",
                        )
                    })?;
                    return super::jira_webhook::process_relayed(&state, &team, &payload)
                        .await
                        .map_err(|code| {
                            fail(code, "handler_error", "Handling the Jira event failed.")
                        });
                }
                // The Forge app was set up with this install's id as its team id:
                // the install's own team takes the event.
                if payload
                    .pointer("/coderhelm/team_id")
                    .and_then(Value::as_str)
                    .is_some_and(sign::is_install_id)
                {
                    let team = only_team(&state).await.ok_or_else(|| {
                        fail(
                            StatusCode::CONFLICT,
                            "no_team",
                            "This install has no team yet. Sign up first.",
                        )
                    })?;
                    payload["coderhelm"]["team_id"] = json!(team);
                }
                let bytes = Bytes::from(serde_json::to_vec(&payload).unwrap_or_default());
                super::jira_webhook::handle_forge(State(state.clone()), bytes)
                    .await
                    .map_err(|code| fail(code, "handler_error", "Handling the Jira event failed."))
            }
            _ => Err(fail(
                StatusCode::BAD_REQUEST,
                "unknown_source",
                "This release doesn't take that kind of event yet. Update CoderHelm.",
            )),
        }
    }
    .await;
    // A copy that wasn't handled isn't "already had": let the relay's retry
    // (or a redelivery) through again.
    // Only a refusal from before the event was handled (the install couldn't
    // fetch the link's settings): a handler that failed partway may have
    // acted already, and isn't run twice.
    if matches!(&handled, Err((code, _)) if *code == StatusCode::SERVICE_UNAVAILABLE) {
        common::claim::release(
            &state.dynamo,
            &state.config.settings_table_name,
            "SYSTEM",
            &sign::replay_key(&stamp.delivery_id),
        )
        .await;
    }
    handled
}

/// What the intake answers for its replay claim: nothing (go on) when this
/// is the first copy; 409 for a copy already taken; 503 when the claim
/// couldn't be written, so a replay is never let through for want of a
/// record (the relay retries a 503).
pub fn intake_claim_answer(c: &common::claim::Claim) -> Option<(StatusCode, &'static str)> {
    match c {
        common::claim::Claim::Won => None,
        common::claim::Claim::Held => Some((
            StatusCode::CONFLICT,
            sign::SignatureProblem::Replayed.code(),
        )),
        common::claim::Claim::Failed(_) => {
            Some((StatusCode::SERVICE_UNAVAILABLE, "store_unavailable"))
        }
    }
}

fn intake_claim_message(code: &str) -> &'static str {
    if code == sign::SignatureProblem::Replayed.code() {
        sign::SignatureProblem::Replayed.message()
    } else {
        "This install couldn't record the delivery just now; the relay tries again."
    }
}

/// The install's team, when it has exactly one (the usual self-hosted case).
///
/// Every page is read: the teams table also holds each team's ticket locks,
/// so one 50-item page could hold no team record at all, or only one of
/// several teams (and that one was then taken as the only team).
async fn only_team(state: &AppState) -> Option<String> {
    let mut ids: Vec<String> = Vec::new();
    let mut start = None;
    loop {
        let r = state
            .dynamo
            .scan()
            .table_name(&state.config.teams_table_name)
            .filter_expression("sk = :m")
            .expression_attribute_values(":m", AttributeValue::S("META".into()))
            .projection_expression("team_id")
            .set_exclusive_start_key(start)
            .send()
            .await
            .ok()?;
        ids.extend(
            r.items()
                .iter()
                .filter_map(|i| i.get("team_id")?.as_s().ok().cloned()),
        );
        if ids.len() > 1 {
            return None;
        }
        start = r.last_evaluated_key().filter(|k| !k.is_empty()).cloned();
        if start.is_none() {
            break;
        }
    }
    sole(ids)
}

/// Pure: the one team, when there is exactly one.
fn sole(mut ids: Vec<String>) -> Option<String> {
    ids.dedup();
    (ids.len() == 1).then(|| ids.remove(0))
}

#[cfg(test)]
mod only_team_tests {
    use super::sole;

    #[test]
    fn exactly_one_team_is_the_install_team() {
        assert_eq!(sole(vec!["TEAM#a".into()]), Some("TEAM#a".into()));
        assert_eq!(sole(vec![]), None);
        assert_eq!(sole(vec!["TEAM#a".into(), "TEAM#b".into()]), None);
    }
}

#[derive(Deserialize, Default)]
pub struct ProofQuery {
    challenge: Option<String>,
}

/// GET /webhooks/relay/proof?challenge= — CoderHelm checks this install
/// holds the registration secret and answers on its CloudFront address.
pub async fn proof(
    ip: crate::middleware::rate_limit::ClientIp,
    Query(q): Query<ProofQuery>,
) -> Result<Json<Value>, StatusCode> {
    crate::middleware::rate_limit::allow(&ip, "hybrid-proof", 30, 60)?;
    let c = creds_now().await.ok_or(StatusCode::NOT_FOUND)?;
    let ch = q.challenge.unwrap_or_default();
    if ch.is_empty() || ch.len() > 100 {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Json(
        json!({ "install_id": c.install_id, "proof": sign::proof(&c.secret, &ch) }),
    ))
}

// ── Sharing with support ─────────────────────────────────────────────────

/// What a shared failure carries: metadata, and the payload preview only
/// when the install chose to share payloads (pure; tested).
pub fn shared_copy(d: &Delivery, payloads: bool) -> Delivery {
    let mut s = d.clone();
    s.headers.clear();
    if !payloads {
        s.payload_preview.clear();
    }
    s
}

/// Sends a failed delivery's metadata to CoderHelm support, when sharing is on.
pub async fn share_failure(state: &AppState, d: &Delivery) {
    let Some(l) = cached_local() else { return };
    if !l.sharing {
        return;
    }
    let Some(c) = client::credentials() else {
        return;
    };
    let copy = shared_copy(d, l.share_payloads);
    let body = json!({ "delivery": copy });
    let call = client::call(
        &state.http,
        &c,
        "POST",
        "/hybrid/deliveries/shared",
        Some(&body),
    );
    if tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .is_err()
    {
        warn!("hybrid: sharing a failure timed out");
    }
}

/// The fixed label of each public hybrid check (the public status never
/// carries addresses or names).
pub fn public_label(id: &str) -> &'static str {
    match id {
        "hybrid_registered" => "Registered with CoderHelm",
        "hybrid_name" => "Your CoderHelm address",
        "hybrid_github" => "CoderHelm's GitHub App",
        "hybrid_access" => "Access to CoderHelm's apps",
        "hybrid_relay" => "Events from CoderHelm's relay",
        "hybrid_jira" => "Jira through CoderHelm",
        _ => "Hybrid",
    }
}

/// The public setup status's hybrid checks (ids, statuses, codes only).
pub async fn public_checks(state: &AppState) -> Vec<Value> {
    let l = local(state).await;
    if l.install_id.is_empty() && l.github_mode.is_empty() {
        return vec![];
    }
    let creds = creds_now().await;
    let hosted = match &creds {
        Some(c) => client::call(&state.http, c, "GET", "/hybrid/status", None)
            .await
            .ok(),
        None => None,
    };
    let code = if l.name.is_empty() {
        None
    } else {
        let (facts, view) = super::setup_checks::domain_facts_now(state).await;
        sync_name_records(state, view.as_ref()).await;
        Some(super::setup_checks::domain_code(&facts).1)
    };
    checks(
        &l,
        creds.is_some(),
        hosted.as_ref(),
        code,
        recent_intake_problem(),
    )
    .iter()
    .map(public_check)
    .collect()
}

/// One hybrid check as the public status shows it: id, status, code and the
/// fixed label (never the title, which can name the site or the address).
pub fn public_check(c: &Value) -> Value {
    let id = c["id"].as_str().unwrap_or("").to_string();
    json!({ "id": id, "status": c["status"], "code": c["code"], "label": public_label(&id), "required": false })
}

/// The `hybrid_jira` check alone, for the public status once someone has
/// signed up: linking a Jira site needs an install admin, so it happens
/// after the setup window closes and the install guide follows it there.
/// `None` when Link through CoderHelm is off, the install isn't registered,
/// or CoderHelm didn't answer (the guide then shows nothing rather than a
/// wrong "not linked"). Kept 10 seconds (3 with `fresh`).
pub async fn public_jira_check(state: &AppState, fresh: bool) -> Option<Value> {
    static KEPT: Mutex<Option<(Instant, Option<Value>)>> = Mutex::new(None);
    let keep = Duration::from_secs(if fresh { 3 } else { 10 });
    if let Some((at, v)) = KEPT.lock().ok().and_then(|g| g.clone()) {
        if at.elapsed() < keep {
            return v;
        }
    }
    let l = local(state).await;
    let v = if !l.jira_link {
        None
    } else {
        match creds_now().await {
            None => None,
            Some(c) => client::call(&state.http, &c, "GET", "/hybrid/status", None)
                .await
                .ok()
                .and_then(|h| jira_public_check(&l, &h)),
        }
    };
    if let Ok(mut g) = KEPT.lock() {
        *g = Some((Instant::now(), v.clone()));
    }
    v
}

/// The public `hybrid_jira` check from CoderHelm's status answer (pure; tested).
pub fn jira_public_check(l: &Local, hosted: &Value) -> Option<Value> {
    checks(l, true, Some(hosted), None, None)
        .iter()
        .find(|c| c["id"] == "hybrid_jira")
        .map(public_check)
}

/// How many GitHub installations are linked to this install (0 when unknown).
pub async fn linked_installations(state: &AppState) -> usize {
    let Some(c) = creds_now().await else { return 0 };
    client::call(&state.http, &c, "GET", "/hybrid/status", None)
        .await
        .ok()
        .and_then(|v| v["github"]["links"].as_array().map(Vec::len))
        .unwrap_or(0)
}

/// The registration to sign in with when this install chose CoderHelm's app
/// for a knowledge provider (Notion, Confluence, Google Drive).
///
/// Reads the hybrid settings and the registration (from this instance's
/// memory, or from the store and the secret when a fresh instance hasn't
/// loaded them yet). It used to read memory only, so on a fresh instance
/// it said "no" and the add flow showed token steps instead of sign-in.
pub async fn brokered(
    state: &AppState,
    kind: common::knowledge::sources::Kind,
) -> Option<Credentials> {
    let l = local(state).await;
    if !brokers(&l, kind) {
        return None;
    }
    creds_now().await
}

/// [`brokered`] for a catalog server signed in with an app (Figma), by its
/// catalog id (the same id the hybrid settings list it under).
pub async fn brokered_id(state: &AppState, id: &str) -> Option<Credentials> {
    let l = local(state).await;
    if !brokers_id(&l, id) {
        return None;
    }
    creds_now().await
}

/// Whether the hybrid settings route `kind` through CoderHelm's app.
fn brokers(l: &Local, kind: common::knowledge::sources::Kind) -> bool {
    brokers_id(l, kind.key())
}

fn brokers_id(l: &Local, id: &str) -> bool {
    l.integrations.iter().any(|i| i == id)
}

/// For the public GitHub checks: is GitHub going through CoderHelm's app?
pub async fn github_hosted(state: &AppState) -> bool {
    local(state).await.github_mode == "hosted"
}

#[cfg(test)]
mod tests {

    fn no_own_apps(_: &str) -> bool {
        false
    }

    #[test]
    fn a_provider_is_brokered_only_when_the_settings_list_it() {
        use common::knowledge::sources::Kind;
        let mut l = Local::default();
        assert!(!brokers(&l, Kind::Notion));
        l.integrations = vec!["github".into(), "notion".into(), "confluence".into()];
        assert!(brokers(&l, Kind::Notion));
        assert!(brokers(&l, Kind::Confluence));
        assert!(!brokers(&l, Kind::GoogleDrive));
    }

    #[test]
    fn every_hosted_app_is_on_with_coderhelms_apps_and_none_when_private() {
        // A new install: nothing chosen, so CoderHelm's apps, for every tool.
        let mut l = Local::default();
        assert!(migrate_apps(&mut l));
        assert_eq!(apps_mode(&l), "coderhelm");
        assert_eq!(
            wanted_integrations(&l, no_own_apps),
            integrations::app_choice_ids().collect::<Vec<_>>()
        );
        // A tool with the install's own app set up uses it either way.
        let own_figma = |id: &str| id == "figma";
        assert!(!uses_coderhelm(&l, "figma", own_figma));
        assert!(uses_coderhelm(&l, "notion", own_figma));
        l.github_mode = "hosted".into();
        assert_eq!(
            wanted_integrations(&l, own_figma),
            vec!["github", "jira", "notion", "confluence", "google_drive"]
        );
        // Fully private: no app through CoderHelm; GitHub keeps its own choice.
        l.apps_mode = "private".into();
        assert_eq!(wanted_integrations(&l, no_own_apps), vec!["github"]);
        l.github_mode = "own".into();
        assert!(wanted_integrations(&l, no_own_apps).is_empty());
    }

    #[test]
    fn a_new_hosted_entry_is_on_with_no_choice_to_make() {
        // Whatever the catalog holds, every hosted entry but GitHub is wanted
        // by an install on CoderHelm's apps: nothing names the tools one by one.
        let l = Local {
            apps_mode: "coderhelm".into(),
            apps_v: 2,
            ..Default::default()
        };
        let hosted: Vec<&str> = integrations::CATALOG
            .iter()
            .filter(|e| e.mode == integrations::Mode::Hosted && e.id != "github")
            .map(|e| e.id)
            .collect();
        assert_eq!(wanted_integrations(&l, no_own_apps), hosted);
        // A record migrated by an earlier release, before an entry was added
        // (Figma here), gets the entry too: per-tool picks are no longer read.
        let mut l = Local {
            apps: [("jira", "coderhelm"), ("notion", "own")]
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .into(),
            apps_v: 1,
            ..Default::default()
        };
        assert!(migrate_apps(&mut l));
        assert!(wanted_integrations(&l, no_own_apps).contains(&"figma".to_string()));
        assert!(wanted_integrations(&l, no_own_apps).contains(&"notion".to_string()));
    }

    #[test]
    fn earlier_per_tool_choices_migrate_once() {
        let picks = |p: &[(&str, &str)]| -> BTreeMap<String, String> {
            p.iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect()
        };
        // Any tool on CoderHelm's app: CoderHelm's apps.
        let mut l = Local {
            apps: picks(&[
                ("jira", "own"),
                ("notion", "coderhelm"),
                ("confluence", "own"),
            ]),
            apps_v: 1,
            ..Default::default()
        };
        assert!(migrate_apps(&mut l));
        assert_eq!(l.apps_mode, "coderhelm");
        assert!(!migrate_apps(&mut l), "only once");
        // Every tool chosen as their own: private.
        let mut l = Local {
            apps: picks(&[
                ("jira", "own"),
                ("notion", "own"),
                ("confluence", "own"),
                ("google_drive", "own"),
            ]),
            apps_v: 1,
            ..Default::default()
        };
        assert!(migrate_apps(&mut l));
        assert_eq!(l.apps_mode, "private");
        assert!(wanted_integrations(&l, no_own_apps).is_empty());
        // Some own, the rest never chosen but held by CoderHelm (the old
        // default): those were on CoderHelm's app, so CoderHelm's apps.
        let mut l = Local {
            apps: picks(&[("notion", "own")]),
            integrations: vec!["jira".into(), "confluence".into()],
            apps_v: 1,
            ..Default::default()
        };
        migrate_apps(&mut l);
        assert_eq!(l.apps_mode, "coderhelm");
        // Some own and nothing else on CoderHelm's app: private.
        let mut l = Local {
            apps: picks(&[("notion", "own"), ("confluence", "own")]),
            integrations: vec!["github".into()],
            apps_v: 1,
            ..Default::default()
        };
        migrate_apps(&mut l);
        assert_eq!(l.apps_mode, "private");
        // A record from before per-tool choices (only the list): any tool on
        // it means CoderHelm's apps; none means never chosen, the default.
        for list in [vec!["github", "notion"], vec!["github"], vec![]] {
            let mut l = Local {
                integrations: list.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            };
            assert!(migrate_apps(&mut l));
            assert_eq!(l.apps_mode, "coderhelm", "{list:?}");
        }
        // A record from before has none of the new fields.
        let old: Local = serde_json::from_str(r#"{"integrations":["github"]}"#).unwrap();
        assert!(old.apps.is_empty() && old.apps_v == 0 && old.apps_mode.is_empty());
        // A choice made already is never overwritten.
        let mut l = Local {
            apps_mode: "private".into(),
            integrations: vec!["notion".into()],
            ..Default::default()
        };
        assert!(migrate_apps(&mut l));
        assert_eq!(l.apps_mode, "private");
    }

    #[test]
    fn the_list_is_sent_again_only_when_it_changed() {
        let mut l = Local::default();
        let want = wanted_integrations(&l, no_own_apps);
        assert!(apps_out_of_date(&l, &want, None), "never sent");
        l.apps_sent = want.clone();
        // CoderHelm took fewer (it doesn't offer Drive): no resend loop.
        l.integrations = vec!["jira".into(), "notion".into(), "confluence".into()];
        assert!(!apps_out_of_date(&l, &want, Some(&l.integrations.clone())));
        // CoderHelm holds something else: send again.
        assert!(apps_out_of_date(&l, &want, Some(&["jira".to_string()])));
        l.apps_mode = "private".into();
        let want = wanted_integrations(&l, no_own_apps);
        assert!(apps_out_of_date(&l, &want, None));
    }

    #[test]
    fn the_guide_and_settings_take_one_choice_and_older_bodies_for_now() {
        let mut l = Local::default();
        let mode = |m: &str| SettingsBody {
            apps_mode: Some(m.into()),
            ..Default::default()
        };
        apply_settings(&mut l, &mode("private")).unwrap();
        assert_eq!(apps_mode(&l), "private");
        apply_settings(&mut l, &mode("coderhelm")).unwrap();
        assert_eq!(apps_mode(&l), "coderhelm");
        assert!(apply_settings(&mut l, &mode("own")).is_err());
        // An older dashboard's per-tool body: any CoderHelm's app means
        // CoderHelm's apps, only "own" means private.
        let apps = |p: &[(&str, &str)]| SettingsBody {
            apps: Some(
                p.iter()
                    .map(|(a, b)| (a.to_string(), b.to_string()))
                    .collect(),
            ),
            ..Default::default()
        };
        apply_settings(&mut l, &apps(&[("notion", "own")])).unwrap();
        assert_eq!(apps_mode(&l), "private");
        apply_settings(&mut l, &apps(&[("notion", "own"), ("jira", "coderhelm")])).unwrap();
        assert_eq!(apps_mode(&l), "coderhelm");
        // Both sent (the guide, for older installs): the new field decides.
        let both = SettingsBody {
            apps_mode: Some("private".into()),
            ..apps(&[("jira", "coderhelm")])
        };
        apply_settings(&mut l, &both).unwrap();
        assert_eq!(apps_mode(&l), "private");
        assert!(apply_settings(&mut l, &apps(&[("github", "own")])).is_err());
        assert!(apply_settings(&mut l, &apps(&[("slack_commands", "coderhelm")])).is_err());
        assert!(apply_settings(&mut l, &apps(&[("notion", "both")])).is_err());
        // The guide's body takes the same fields.
        let g =
            guide_body(br#"{"setup_token":"t","apps_mode":"private","apps":{"confluence":"own"}}"#);
        assert_eq!(g.apps_mode.as_deref(), Some("private"));
        assert_eq!(
            g.apps.unwrap().get("confluence").map(String::as_str),
            Some("own")
        );
    }

    #[test]
    fn the_intake_never_takes_a_copy_it_couldnt_record() {
        use common::claim::Claim;
        assert_eq!(intake_claim_answer(&Claim::Won), None);
        assert_eq!(
            intake_claim_answer(&Claim::Held).map(|a| a.0),
            Some(StatusCode::CONFLICT)
        );
        // A failed write used to let the event through: a replay could land
        // while the store was throttled.
        assert_eq!(
            intake_claim_answer(&Claim::Failed("throttled".into())).map(|a| a.0),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
    }

    use super::*;

    mod coderhelm_signature {
        use super::*;
        use base64::Engine;
        use ed25519_dalek::{Signer, SigningKey};

        const INSTALL: &str = "ins_abcdefghijklmnopqrstuvwx";
        const NOW: i64 = 1_761_436_800;

        fn key() -> SigningKey {
            SigningKey::from_bytes(&[7; 32])
        }

        fn pinned() -> hosted_sig::TrustedKeys {
            let b = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(key().verifying_key().to_bytes());
            hosted_sig::TrustedKeys::parse(&b).unwrap()
        }

        fn signed_headers(install: &str, d: &str, ts: i64) -> HeaderMap {
            let kid = hosted_sig::kid_of(&key().verifying_key().to_bytes());
            let m = hosted_sig::Message {
                install_id: install,
                ts,
                delivery_id: d,
                source: "github",
                event: "push",
                body: b"{}",
            };
            let sig = key().sign(&hosted_sig::canonical(&kid, &m).unwrap());
            let mut h = HeaderMap::new();
            h.insert(
                hosted_sig::HOSTED_HEADER,
                hosted_sig::header(&kid, &m, &sig.to_bytes())
                    .parse()
                    .unwrap(),
            );
            h
        }

        fn stamp() -> sign::RelayStamp {
            sign::RelayStamp {
                delivery_id: "d-1".into(),
                ts: NOW,
            }
        }

        #[test]
        fn a_release_without_pinned_keys_keeps_the_relay_check() {
            let none = hosted_sig::TrustedKeys::default();
            assert!(hosted_signed_with(
                &none,
                &HeaderMap::new(),
                INSTALL,
                "github",
                "push",
                b"{}",
                &stamp(),
                NOW
            )
            .is_ok());
        }

        #[test]
        fn once_keys_are_pinned_an_unsigned_event_is_refused() {
            assert_eq!(
                hosted_signed_with(
                    &pinned(),
                    &HeaderMap::new(),
                    INSTALL,
                    "github",
                    "push",
                    b"{}",
                    &stamp(),
                    NOW
                ),
                Err(sign::SignatureProblem::Missing)
            );
        }

        #[test]
        fn coderhelms_signature_for_this_install_and_delivery_passes() {
            let h = signed_headers(INSTALL, "d-1", NOW);
            assert!(hosted_signed_with(
                &pinned(),
                &h,
                INSTALL,
                "github",
                "push",
                b"{}",
                &stamp(),
                NOW
            )
            .is_ok());
        }

        #[test]
        fn a_signature_for_another_install_or_delivery_is_refused() {
            let other = signed_headers("ins_bbbbbbbbbbbbbbbbbbbbbbbb", "d-1", NOW);
            assert_eq!(
                hosted_signed_with(
                    &pinned(),
                    &other,
                    INSTALL,
                    "github",
                    "push",
                    b"{}",
                    &stamp(),
                    NOW
                ),
                Err(sign::SignatureProblem::Mismatch)
            );
            let moved = signed_headers(INSTALL, "d-2", NOW);
            assert_eq!(
                hosted_signed_with(
                    &pinned(),
                    &moved,
                    INSTALL,
                    "github",
                    "push",
                    b"{}",
                    &stamp(),
                    NOW
                ),
                Err(sign::SignatureProblem::Mismatch)
            );
        }
    }

    fn registered() -> Local {
        Local {
            install_id: "ins_abcdefghijklmnopqrstuvwx".into(),
            verified_at: "t".into(),
            ..Default::default()
        }
    }

    #[test]
    fn the_jira_link_is_a_setting_and_follows_coderhelms_link() {
        let mut l = Local::default();
        assert!(!l.jira_link, "off by default");
        let on = SettingsBody {
            jira_link: Some(true),
            ..Default::default()
        };
        apply_settings(&mut l, &on).unwrap();
        assert!(l.jira_link);
        let link = json!({ "team_id": "TEAM#1", "site_url": "https://example.atlassian.net", "linked_at": "2026-10-30T00:00:00Z" });
        // Linked on CoderHelm, not copied yet: copy.
        assert_eq!(jira_sync_plan(&l, Some(&link)), JiraSync::Copy);
        l.jira_synced_at = "2026-10-30T00:00:00Z".into();
        assert_eq!(jira_sync_plan(&l, Some(&link)), JiraSync::Nothing);
        // Linked again (a new link): copy again.
        let relinked = json!({ "team_id": "TEAM#1", "linked_at": "2026-10-31T00:00:00Z" });
        assert_eq!(jira_sync_plan(&l, Some(&relinked)), JiraSync::Copy);
        // Unlinked on CoderHelm's side: forget the copied settings.
        assert_eq!(jira_sync_plan(&l, Some(&Value::Null)), JiraSync::Forget);
        assert_eq!(jira_sync_plan(&l, None), JiraSync::Forget);
        // The setting off: a link CoderHelm still reports is never copied.
        let mut off = l.clone();
        off.jira_link = false;
        assert_eq!(jira_sync_plan(&off, Some(&link)), JiraSync::Forget);
        off.jira_synced_at.clear();
        assert_eq!(jira_sync_plan(&off, Some(&link)), JiraSync::Nothing);
    }

    #[test]
    fn a_link_keeps_the_installs_own_jira_settings_aside() {
        let item = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
                .collect::<std::collections::HashMap<_, _>>()
        };
        let own = item(&[
            ("forge_secret", "own"),
            ("add_comment_url", "https://x.example.net/hook"),
        ]);
        assert!(own_jira_settings(&own));
        let copied = item(&[
            ("forge_secret", "token"),
            (
                "add_comment_url",
                "https://api.example.com/hybrid/jira/trigger/ins_a/add_comment/t",
            ),
        ]);
        assert!(!own_jira_settings(&copied));
        assert!(!own_jira_settings(&item(&[])));
        let f = jira_fields();
        assert!(
            f.contains(&"forge_secret")
                && f.contains(&"site_url")
                && f.contains(&"add_comment_url")
        );
        assert_eq!(f.len(), 8);
    }

    #[test]
    fn the_jira_check_shows_the_link() {
        let mut l = Local {
            verified_at: "x".into(),
            jira_link: true,
            ..Default::default()
        };
        let hosted = json!({ "verified": true, "jira": { "link": null } });
        let row = |l: &Local, h: &Value| {
            checks(l, true, Some(h), None, None)
                .into_iter()
                .find(|c| c["id"] == "hybrid_jira")
        };
        let c = row(&l, &hosted).unwrap();
        assert_eq!(
            (c["status"].as_str(), c["code"].as_str()),
            (Some("todo"), Some("no_link"))
        );
        assert!(c["fix"]
            .as_str()
            .unwrap()
            .contains("Settings, Integrations, Jira"));
        let linked = json!({ "verified": true, "jira": { "link": { "team_id": "TEAM#1", "site_url": "https://example.atlassian.net", "linked_at": "t1" } } });
        let c = row(&l, &linked).unwrap();
        assert_eq!(c["code"], "syncing");
        l.jira_synced_at = "t1".into();
        let c = row(&l, &linked).unwrap();
        assert_eq!(
            (c["status"].as_str(), c["code"].as_str()),
            (Some("pass"), Some("ok"))
        );
        assert!(c["title"]
            .as_str()
            .unwrap()
            .contains("example.atlassian.net"));
        // The public form (after sign-up): codes and the fixed label only,
        // never the site.
        let p = jira_public_check(&l, &linked).unwrap();
        assert_eq!(
            (p["code"].as_str(), p["status"].as_str()),
            (Some("ok"), Some("pass"))
        );
        assert_eq!(p["label"], "Jira through CoderHelm");
        assert_eq!(p["required"], false);
        assert!(!p.to_string().contains("atlassian"));
        l.jira_synced_at.clear();
        assert_eq!(jira_public_check(&l, &linked).unwrap()["code"], "syncing");
        assert_eq!(jira_public_check(&l, &hosted).unwrap()["code"], "no_link");
        // Off: no Jira row at all.
        l.jira_link = false;
        assert!(row(&l, &linked).is_none());
        assert!(jira_public_check(&l, &linked).is_none());
        assert_eq!(public_label("hybrid_jira"), "Jira through CoderHelm");
    }

    #[test]
    fn settings_keep_github_and_sharing_consistent() {
        let mut l = Local::default();
        let b = SettingsBody {
            github_mode: Some("hosted".into()),
            integrations: Some(vec!["notion".into()]),
            sharing: Some(SharingBody {
                enabled: false,
                payloads: true,
            }),
            jira_link: None,
            apps_mode: None,
            apps: None,
        };
        apply_settings(&mut l, &b).unwrap();
        // The whole list from an older dashboard: a tool on it, CoderHelm's apps.
        assert_eq!(apps_mode(&l), "coderhelm");
        assert_eq!(wanted_integrations(&l, no_own_apps)[0], "github");
        assert!(!l.share_payloads, "payloads only with sharing on");
        // None of the tools on that list: private.
        let b = SettingsBody {
            github_mode: Some("own".into()),
            integrations: Some(vec![]),
            ..Default::default()
        };
        apply_settings(&mut l, &b).unwrap();
        assert_eq!(apps_mode(&l), "private");
        assert!(wanted_integrations(&l, no_own_apps).is_empty());
        let bad = SettingsBody {
            integrations: Some(vec!["slack_commands".into()]),
            ..Default::default()
        };
        assert!(apply_settings(&mut l, &bad).is_err());
        let bad = SettingsBody {
            github_mode: Some("both".into()),
            ..Default::default()
        };
        assert!(apply_settings(&mut l, &bad).is_err());
        let on = SettingsBody {
            sharing: Some(SharingBody {
                enabled: true,
                payloads: true,
            }),
            ..Default::default()
        };
        apply_settings(&mut l, &on).unwrap();
        assert!(l.sharing && l.share_payloads);
    }

    #[test]
    fn checks_say_what_to_do_next() {
        let c = checks(&Local::default(), false, None, None, None);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0]["code"], "not_registered");
        let mut l = registered();
        l.name = "example".into();
        l.github_mode = "hosted".into();
        let hosted = json!({"verified": true, "status": "active", "github": {"links": []}});
        let c = checks(&l, true, Some(&hosted), Some("validating"), None);
        let by = |id: &str| c.iter().find(|x| x["id"] == id).cloned().unwrap();
        assert_eq!(by("hybrid_registered")["status"], "pass");
        assert_eq!(by("hybrid_name")["code"], "cert_pending");
        assert!(by("hybrid_name")["title"]
            .as_str()
            .unwrap()
            .contains("example.coderhelm.com"));
        assert_eq!(by("hybrid_github")["code"], "no_link");
        let hosted = json!({"verified": false, "status": "revoked", "github": {"links": [{"installation_id": 1}]}});
        let c = checks(
            &l,
            true,
            Some(&hosted),
            Some("ok"),
            Some("signature_mismatch"),
        );
        let by = |id: &str| c.iter().find(|x| x["id"] == id).cloned().unwrap();
        assert_eq!(by("hybrid_registered")["code"], "proof_failed");
        assert_eq!(by("hybrid_access")["code"], "broker_revoked");
        assert_eq!(by("hybrid_name")["status"], "pass");
        assert_eq!(by("hybrid_github")["status"], "pass");
        assert_eq!(by("hybrid_relay")["code"], "signature_mismatch");
        assert!(c
            .iter()
            .all(|x| x["title"].as_str().is_some_and(|t| !t.is_empty())));
    }

    #[test]
    fn only_the_claimed_names_records_are_sent() {
        let view = json!({
            "hostname": "example.coderhelm.com",
            "state": "live",
            "records": [
                {"purpose": "certificate", "name": "_abc.example.coderhelm.com.", "type": "CNAME", "value": "_def.acm-validations.aws."},
                {"purpose": "address", "name": "example.coderhelm.com", "type": "CNAME", "value": "d1abcdef234567.cloudfront.net"}
            ]
        });
        let r = records_for(&view, "example");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].name, "_abc.example.coderhelm.com");
        assert_eq!(r[0].value, "_def.acm-validations.aws");
        assert!(records_for(&view, "other").is_empty());
        let mut early = view.clone();
        early["state"] = json!("validating");
        let r = records_for(&early, "example");
        assert_eq!(r.len(), 1, "the address waits for the certificate");
        assert_eq!(r[0].purpose, "certificate");
        assert!(records_for(&view, "").is_empty());
    }

    #[test]
    fn shared_failures_carry_metadata_and_payloads_only_when_chosen() {
        let d = Delivery::new(Direction::Inbound, "github", "push", "TEAM#1")
            .with_headers([("X-GitHub-Event", "push")])
            .with_payload(br#"{"a":1}"#);
        let m = shared_copy(&d, false);
        assert!(m.headers.is_empty() && m.payload_preview.is_empty());
        assert_eq!(m.event, "push");
        let p = shared_copy(&d, true);
        assert!(!p.payload_preview.is_empty() && p.headers.is_empty());
    }
}
