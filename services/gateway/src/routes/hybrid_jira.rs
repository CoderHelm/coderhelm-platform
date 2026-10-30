//! The hosted side of hybrid's Jira link: a hosted team's Jira site, linked
//! to a self-hosted install, so CoderHelm forwards that site's Jira events
//! (the Forge app's issue and comment events, and plain Jira webhooks) to
//! the install through the relay instead of handling them here.
//!
//! Linking proves both sides:
//!
//! 1. An install admin asks (signed by the install): `POST
//!    /hybrid/jira/link-start {site_url, return_url}` names the Jira site
//!    they expect, and answers a one-hour state and the CoderHelm page.
//! 2. On that page (app.coderhelm.com, Settings, Integrations, Jira) the
//!    person is signed in to CoderHelm as an admin of the team that
//!    connected the site with CoderHelm's Jira app (never a support view or
//!    an API key). The page names the install's address, when it registered,
//!    and the site. The team's site must be the one the install asked for:
//!    a state someone else got hold of can't pair another team's site.
//! 3. Pressing Link (`POST /api/hybrid/jira-link/:state`) takes the state
//!    once and writes the link and its route together, never over another
//!    install's link.
//!
//! From then on the Jira intake checks each event first (the Forge secret or
//! the webhook's signature, exactly as for a hosted team), then forwards it
//! ([`relay_team`]) without the Forge secret, and stops: hosted never acts on
//! an event it relays.
//!
//! The install never gets the site's Forge secret or its Forge links. Its
//! Jira app settings point at this side's proxy instead
//! (`/hybrid/jira/trigger/<install>/<trigger>/<token>`, [`trigger`]): each
//! link carries a token for that install and trigger, and the proxy calls
//! the site's real Forge link with the real secret only while the link
//! stands. Unlinking (from the install, from the team on CoderHelm, by the
//! install leaving hybrid, or by revocation) removes both records, so the
//! proxy refuses and the site's events are handled here again.
//!
//! Records on the settings table: `HYBRID#<install>`/`JIRALINK` (the link,
//! one site per install) and `HYBRIDJIRA#<team>`/`LINK` (the relay's route).

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::{AttributeValue, Delete, Put, TransactWriteItem};
use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use common::deliveries::{self, Delivery, Direction};
use common::hybrid::{broker, names, relay, sign};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Digest;
use subtle::ConstantTimeEq;

use super::hybrid::{
    audit, body_json, epoch, fail, forward_and_record, load_install, master, not_here, now,
    put_state, return_url_for, s, signed, take_state, Install,
};
use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// The link's sort key under the install (one site per install).
pub const LINK_SK: &str = "JIRALINK";

/// The relay's route for a hosted team's Jira site.
pub fn reverse_pk(team_id: &str) -> String {
    format!("HYBRIDJIRA#{team_id}")
}

fn state_pk(st: &str) -> String {
    format!("HYBRIDJSTATE#{st}")
}

/// A well-formed link state (`hyj_` and the handle).
pub fn is_link_state(st: &str) -> bool {
    st.starts_with("hyj_")
        && st.len() <= 60
        && st[4..]
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// The site's host from what a person types or Forge reports
/// (`https://example.atlassian.net/`, `example.atlassian.net`), lowercase.
/// None for anything that isn't a plain host name.
pub fn site_host(raw: &str) -> Option<String> {
    let t = raw.trim().to_ascii_lowercase();
    let t = t
        .strip_prefix("https://")
        .or_else(|| t.strip_prefix("http://"))
        .unwrap_or(&t);
    let host = t.split(['/', '?', '#']).next().unwrap_or("");
    let ok = !host.is_empty()
        && host.len() <= 253
        && host.contains('.')
        && host
            .split('.')
            .all(|l| !l.is_empty() && l.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
    ok.then(|| host.to_string())
}

/// One linked site, as the install sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JiraLink {
    pub team_id: String,
    /// The Jira site's address (`https://example.atlassian.net`).
    #[serde(default)]
    pub site_url: String,
    /// Who linked it (kept on this side; never sent to the install).
    #[serde(default, skip_serializing)]
    pub linked_by: String,
    #[serde(default)]
    pub linked_at: String,
}

fn txt(i: &HashMap<String, AttributeValue>, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// The site linked to an install, if any.
pub async fn jira_link(state: &AppState, install_id: &str) -> Option<JiraLink> {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&relay::install_pk(install_id)))
        .key("sk", s(LINK_SK))
        .consistent_read(true)
        .send()
        .await
        .ok()?;
    let i = r.item()?;
    Some(JiraLink {
        team_id: txt(i, "team_id"),
        site_url: txt(i, "site_url"),
        linked_by: txt(i, "linked_by"),
        linked_at: txt(i, "linked_at"),
    })
}

/// The install a hosted team's Jira site is routed to, if any.
pub async fn install_for_team(state: &AppState, team_id: &str) -> Option<String> {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&reverse_pk(team_id)))
        .key("sk", s("LINK"))
        .consistent_read(true)
        .send()
        .await
        .ok()?;
    r.item()?.get("install_id")?.as_s().ok().cloned()
}

/// The link and its route deleted together: the link only while it is this
/// team's, the route only while it points at this install.
pub fn unlink_writes(
    table: &str,
    install_id: &str,
    team_id: &str,
) -> Result<Vec<TransactWriteItem>, aws_sdk_dynamodb::error::BuildError> {
    let own = Delete::builder()
        .table_name(table)
        .key("pk", s(&relay::install_pk(install_id)))
        .key("sk", s(LINK_SK))
        .condition_expression("attribute_not_exists(pk) OR team_id = :t")
        .expression_attribute_values(":t", s(team_id))
        .build()?;
    let reverse = Delete::builder()
        .table_name(table)
        .key("pk", s(&reverse_pk(team_id)))
        .key("sk", s("LINK"))
        .condition_expression("attribute_not_exists(pk) OR install_id = :i")
        .expression_attribute_values(":i", s(install_id))
        .build()?;
    Ok(vec![
        TransactWriteItem::builder().delete(own).build(),
        TransactWriteItem::builder().delete(reverse).build(),
    ])
}

async fn unlink_pair(state: &AppState, install_id: &str, team_id: &str, by: &str) -> bool {
    let ok = match unlink_writes(&state.config.settings_table_name, install_id, team_id) {
        Ok(items) => state
            .dynamo
            .transact_write_items()
            .set_transact_items(Some(items))
            .send()
            .await
            .is_ok(),
        Err(_) => false,
    };
    if ok {
        audit(
            state,
            install_id,
            "unlink_jira",
            json!({ "team_id": team_id, "by": by }),
        )
        .await;
    }
    ok
}

/// Removes an install's link and its route, so the site's events are
/// handled here again.
pub async fn unlink_all(state: &AppState, install_id: &str) {
    if let Some(link) = jira_link(state, install_id).await {
        unlink_pair(state, install_id, &link.team_id, "install").await;
    }
}

/// Removes a team's route and the link it points at (the team's own unlink,
/// and the clean-up for a route left without its link).
pub async fn unlink_team(state: &AppState, team_id: &str, by: &str) -> bool {
    match install_for_team(state, team_id).await {
        Some(install_id) => unlink_pair(state, &install_id, team_id, by).await,
        None => true,
    }
}

/// What the hosted team has connected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TeamJira {
    pub site_url: String,
    /// CoderHelm's Jira app registered its links from the site with the
    /// team's secret: the proof the team runs the app on that site.
    pub forge: bool,
}

async fn team_jira(state: &AppState, team_id: &str) -> TeamJira {
    let config = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", s(team_id))
        .key("sk", s("JIRA#config"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item);
    let g = |k: &str| config.as_ref().map(|i| txt(i, k)).unwrap_or_default();
    TeamJira {
        site_url: g("site_url"),
        forge: !g("add_comment_url").is_empty()
            && !g("forge_secret").is_empty()
            && site_host(&g("site_url")).is_some(),
    }
}

/// Why a site can't be linked to this install right now (pure; tested).
///
/// - the install must be registered, active and verified;
/// - the team must run CoderHelm's Jira app on a site (its registration is
///   the proof; a webhook token alone isn't);
/// - the team's site must be the one the install asked for;
/// - one install per site, one site per install.
pub fn link_blocked(
    install: Option<&Install>,
    team: &TeamJira,
    expected_site: &str,
    route: Option<&str>,
    current: Option<&JiraLink>,
    team_id: &str,
) -> Option<&'static str> {
    let Some(i) = install.filter(|i| i.verified()) else {
        return Some("install_off");
    };
    if !team.forge {
        return Some("jira_not_connected");
    }
    if site_host(&team.site_url).is_none() || site_host(&team.site_url) != site_host(expected_site)
    {
        return Some("site_mismatch");
    }
    if route.is_some_and(|r| r != i.id) {
        return Some("linked_elsewhere");
    }
    if current.is_some_and(|l| l.team_id != team_id) {
        return Some("install_has_other_site");
    }
    None
}

/// The plain words for a refusal code, for the CoderHelm page.
pub fn blocked_message(code: &str) -> &'static str {
    match code {
        "install_off" => "That install isn't registered and verified with CoderHelm any more. Start again from the install.",
        "jira_not_connected" => "This team doesn't run CoderHelm's Jira app on a Jira site yet. Set the app up from this page first, then start again from the install.",
        "site_mismatch" => "The install asked for a different Jira site than the one this team connected. Check the site address on the install and start again, or sign in to the team that connected that site.",
        "linked_elsewhere" => "This team's Jira site is already linked to another self-hosted install. Unlink it there or on this page first.",
        "install_has_other_site" => "That install is already linked to another team's Jira site. Unlink it on the install first.",
        "expired" => "That link expired or was already used. Start it again from the install.",
        _ => "This site can't be linked right now.",
    }
}

/// The link and its route, written together or not at all: never over
/// another install's route, never over another team's link.
pub fn link_writes(
    table: &str,
    install_id: &str,
    link: &JiraLink,
) -> Result<Vec<TransactWriteItem>, aws_sdk_dynamodb::error::BuildError> {
    let own = Put::builder()
        .table_name(table)
        .item("pk", s(&relay::install_pk(install_id)))
        .item("sk", s(LINK_SK))
        .item("team_id", s(&link.team_id))
        .item("site_url", s(&link.site_url))
        .item("linked_by", s(&link.linked_by))
        .item("linked_at", s(&link.linked_at))
        .condition_expression("attribute_not_exists(pk) OR team_id = :t")
        .expression_attribute_values(":t", s(&link.team_id))
        .build()?;
    let reverse = Put::builder()
        .table_name(table)
        .item("pk", s(&reverse_pk(&link.team_id)))
        .item("sk", s("LINK"))
        .item("install_id", s(install_id))
        .item("linked_at", s(&link.linked_at))
        .condition_expression("attribute_not_exists(pk) OR install_id = :i")
        .expression_attribute_values(":i", s(install_id))
        .build()?;
    Ok(vec![
        TransactWriteItem::builder().put(own).build(),
        TransactWriteItem::builder().put(reverse).build(),
    ])
}

/// The address the CoderHelm page names: the install's CoderHelm name, or
/// its CloudFront address.
fn install_address(i: &Install) -> String {
    if i.name.is_empty() {
        i.cloudfront.clone()
    } else {
        names::hostname(&i.name)
    }
}

/// `return_url` with `hybrid_jira=<outcome>` added before any fragment.
pub fn with_outcome(base: &str, outcome: &str) -> String {
    let (path, frag) = base
        .split_once('#')
        .map(|(p, f)| (p.to_string(), format!("#{f}")))
        .unwrap_or((base.to_string(), String::new()));
    let sep = if path.contains('?') { '&' } else { '?' };
    format!("{path}{sep}hybrid_jira={outcome}{frag}")
}

// ── The install's side of linking (signed) ───────────────────────────────

/// POST /hybrid/jira/link-start (signed) `{site_url, return_url}`: a one-hour
/// state for the site the install expects, and the CoderHelm page where an
/// admin of the team that connected it confirms.
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
    let b = body_json(&body);
    let site = site_host(b["site_url"].as_str().unwrap_or("")).ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            "bad_site",
            "Send the Jira site's address, e.g. example.atlassian.net.",
        )
    })?;
    let ret = return_url_for(&install, b["return_url"].as_str().unwrap_or(""));
    let st = broker::new_handle("hyj");
    put_state(
        &state,
        &state_pk(&st),
        &[
            ("install_id", &install.id),
            ("return_url", &ret),
            ("site", &site),
        ],
        3600,
    )
    .await?;
    audit(
        &state,
        &install.id,
        "jira_link_start",
        json!({ "site": site }),
    )
    .await;
    Ok(Json(json!({
        "state": st,
        "link_url": format!(
            "{}/settings/integrations/jira/?hybrid_link={st}",
            common::public_urls::app_url(&state.config.stage)
        ),
    })))
}

/// DELETE /hybrid/jira/link (signed): unlink; the site's events come back here.
pub async fn delete_link(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    unlink_all(&state, &install.id).await;
    Ok(Json(json!({ "ok": true })))
}

/// The Forge triggers a linked install may call through the proxy: the
/// Jira settings field it reads and the trigger's name in the proxy path.
pub const TRIGGERS: &[(&str, &str)] = &[
    ("list_projects_url", "list_projects"),
    ("create_ticket_url", "create_ticket"),
    ("add_comment_url", "add_comment"),
    ("publish_release_url", "publish_release"),
    ("get_issues_url", "get_issues"),
    ("confluence_trigger_url", "confluence"),
];

/// The proxy link the install calls for one trigger.
pub fn proxy_url(api: &str, master: &str, install_id: &str, trigger: &str) -> String {
    format!(
        "{}/hybrid/jira/trigger/{install_id}/{trigger}/{}",
        api.trim_end_matches('/'),
        sign::jira_trigger_token(master, install_id, trigger)
    )
}

/// The link stands both ways: the install's link names the team, and the
/// team's route names the install.
async fn link_stands(state: &AppState, install_id: &str) -> Option<JiraLink> {
    let link = jira_link(state, install_id).await?;
    (install_for_team(state, &link.team_id).await.as_deref() == Some(install_id)).then_some(link)
}

/// POST /hybrid/jira/config (signed): the Jira app settings the install
/// uses for the linked site: its address and, for each Forge trigger the
/// site registered, a proxy link on this side. Never the site's Forge secret
/// or its own links.
pub async fn config(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = signed(&state, &headers, &method, &uri, &body).await?;
    let master = master(&state).ok_or_else(not_here)?.to_string();
    if !install.verified() {
        return Err(fail(
            StatusCode::CONFLICT,
            "not_verified",
            "Verify the install first.",
        ));
    }
    let link = link_stands(&state, &install.id).await.ok_or_else(|| {
        fail(
            StatusCode::NOT_FOUND,
            "jira_not_linked",
            "No Jira site is linked to this install.",
        )
    })?;
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", s(&link.team_id))
        .key("sk", s("JIRA#config"))
        .send()
        .await
        .map_err(|_| {
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?
        .item
        .unwrap_or_default();
    let api = common::public_urls::api_url(&state.config.stage);
    let mut out = json!({
        "linked_at": link.linked_at,
        "site_url": txt(&item, "site_url"),
        // What the install's own checks see as the app's secret: a token for
        // this install, never the site's.
        "forge_secret": sign::jira_trigger_token(&master, &install.id, "forge"),
    });
    for (field, trigger) in TRIGGERS {
        out[*field] = if txt(&item, field).is_empty() {
            json!("")
        } else {
            json!(proxy_url(api, &master, &install.id, trigger))
        };
    }
    Ok(Json(out))
}

/// How much of a Forge trigger's answer the proxy passes back.
const MAX_TRIGGER_ANSWER: usize = 1024 * 1024;

/// Whether a stored Forge link may be called: https only.
pub fn callable(url: &str) -> bool {
    url::Url::parse(url).is_ok_and(|u| u.scheme() == "https" && u.host_str().is_some())
}

/// The body a trigger call goes on with: the site's real Forge secret in
/// place of whatever the install sent as `forge_secret`; anything else
/// unchanged (pure; tested).
pub fn with_site_secret(body: &[u8], secret: &str) -> Vec<u8> {
    match serde_json::from_slice::<Value>(body) {
        Ok(mut v) if v.get("forge_secret").is_some() => {
            v["forge_secret"] = json!(secret);
            serde_json::to_vec(&v).unwrap_or_default()
        }
        _ => body.to_vec(),
    }
}

/// POST /hybrid/jira/trigger/:install/:trigger/:token: a linked install's
/// call to one of its site's Forge triggers (a comment, a ticket, a release,
/// the project list). Taken only with the install's token for that trigger,
/// from an active, verified install whose link still stands; sent on to the
/// site's own trigger with the site's secret, and its answer passed back.
pub async fn trigger(
    State(state): State<Arc<AppState>>,
    Path((install_id, trigger, token)): Path<(String, String, String)>,
    body: Bytes,
) -> Response {
    let Some(master) = master(&state) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let refuse =
        |code: StatusCode, why: &str| (code, Json(json!({ "error": why }))).into_response();
    let Some((field, _)) = TRIGGERS.iter().find(|(_, t)| *t == trigger) else {
        return refuse(StatusCode::NOT_FOUND, "Unknown trigger.");
    };
    if !sign::is_install_id(&install_id) {
        return refuse(StatusCode::NOT_FOUND, "Unknown install.");
    }
    let want = sign::jira_trigger_token(master, &install_id, &trigger);
    if !bool::from(want.as_bytes().ct_eq(token.as_bytes())) {
        return refuse(StatusCode::FORBIDDEN, "Wrong link.");
    }
    match load_install(&state, &install_id).await {
        Ok(Some(i)) if i.verified() => {}
        _ => return refuse(StatusCode::FORBIDDEN, "This install's access is off."),
    }
    let Some(link) = link_stands(&state, &install_id).await else {
        return refuse(
            StatusCode::FORBIDDEN,
            "No Jira site is linked to this install.",
        );
    };
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", s(&link.team_id))
        .key("sk", s("JIRA#config"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .unwrap_or_default();
    let (url, secret) = (txt(&item, field), txt(&item, "forge_secret"));
    if !callable(&url) {
        return refuse(
            StatusCode::NOT_FOUND,
            "The site hasn't registered that trigger.",
        );
    }
    let sent = state
        .http
        .post(&url)
        .header("Content-Type", "application/json")
        .timeout(std::time::Duration::from_secs(25))
        .body(with_site_secret(&body, &secret))
        .send()
        .await;
    match sent {
        Ok(r) => {
            let code = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let bytes = r.bytes().await.unwrap_or_default();
            let bytes = bytes.slice(..bytes.len().min(MAX_TRIGGER_ANSWER));
            (code, [("content-type", "application/json")], bytes).into_response()
        }
        Err(_) => refuse(StatusCode::BAD_GATEWAY, "The Jira site didn't answer."),
    }
}

// ── The team admin's side (CoderHelm's own dashboard) ────────────────────

/// A team admin, signed in as themselves: never a support view of the team
/// and never an API key (linking hands the site's events to another
/// deployment, so a person presses it).
fn team_admin(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    if claims.support_view.is_some() || claims.api_key.is_some() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Sign in as an admin of this team to link its Jira site.",
        ));
    }
    claims.require_role(3).map_err(|c| {
        fail(
            c,
            "forbidden",
            "Only an admin of this team can link its Jira site.",
        )
    })
}

/// Reads a state without taking it. None when unknown or expired.
async fn peek_state(state: &AppState, st: &str) -> Option<HashMap<String, AttributeValue>> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&state_pk(st)))
        .key("sk", s("STATE"))
        .consistent_read(true)
        .send()
        .await
        .ok()?
        .item?;
    let expires: i64 = item
        .get("expires")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    (expires >= epoch()).then_some(item)
}

/// GET /api/hybrid/jira-link/:state (team admins): what pressing Link would
/// do: the install's address and registration date, this team's site, the
/// site the install asked for, and anything in the way.
pub async fn link_view(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(st): Path<String>,
) -> Reply {
    master(&state).ok_or_else(not_here)?;
    team_admin(&claims)?;
    if !is_link_state(&st) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "bad_state",
            blocked_message("expired"),
        ));
    }
    let p = peek_state(&state, &st)
        .await
        .ok_or_else(|| fail(StatusCode::GONE, "expired", blocked_message("expired")))?;
    let install_id = txt(&p, "install_id");
    let install = load_install(&state, &install_id).await?;
    let team = team_jira(&state, &claims.team_id).await;
    let route = install_for_team(&state, &claims.team_id).await;
    let current = jira_link(&state, &install_id).await;
    let blocked = link_blocked(
        install.as_ref(),
        &team,
        &txt(&p, "site"),
        route.as_deref(),
        current.as_ref(),
        &claims.team_id,
    );
    Ok(Json(json!({
        "install_address": install.as_ref().map(install_address),
        "install_registered_at": install.as_ref().map(|i| i.created_at.clone()),
        "site_url": team.site_url,
        "expected_site": txt(&p, "site"),
        "forge": team.forge,
        "blocked": blocked,
        "message": blocked.map(blocked_message),
    })))
}

#[derive(Deserialize, Default)]
pub struct ConfirmBody {
    #[serde(default)]
    choice: String,
}

/// POST /api/hybrid/jira-link/:state `{choice: link|cancel}` (team admins):
/// the answer. Takes the state once; only `link` links. Answers where to go
/// back to on the install.
pub async fn link_confirm(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(st): Path<String>,
    Json(b): Json<ConfirmBody>,
) -> Reply {
    master(&state).ok_or_else(not_here)?;
    team_admin(&claims)?;
    if !is_link_state(&st) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "bad_state",
            blocked_message("expired"),
        ));
    }
    let p = take_state(&state, &state_pk(&st))
        .await
        .ok_or_else(|| fail(StatusCode::GONE, "expired", blocked_message("expired")))?;
    let (install_id, ret) = (txt(&p, "install_id"), txt(&p, "return_url"));
    if b.choice != "link" {
        audit(
            &state,
            &install_id,
            "jira_link_declined",
            json!({ "team_id": claims.team_id }),
        )
        .await;
        return Ok(Json(
            json!({ "outcome": "declined", "return_url": with_outcome(&ret, "declined") }),
        ));
    }
    let install = load_install(&state, &install_id).await?;
    let team = team_jira(&state, &claims.team_id).await;
    let route = install_for_team(&state, &claims.team_id).await;
    let current = jira_link(&state, &install_id).await;
    if let Some(code) = link_blocked(
        install.as_ref(),
        &team,
        &txt(&p, "site"),
        route.as_deref(),
        current.as_ref(),
        &claims.team_id,
    ) {
        audit(
            &state,
            &install_id,
            "jira_link_refused",
            json!({ "team_id": claims.team_id, "why": code }),
        )
        .await;
        return Err(fail(StatusCode::CONFLICT, code, blocked_message(code)));
    }
    let link = JiraLink {
        team_id: claims.team_id.clone(),
        site_url: team.site_url.clone(),
        linked_by: claims.sub.clone(),
        linked_at: now(),
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
        return Err(fail(
            StatusCode::CONFLICT,
            "linked_elsewhere",
            "The site or the install was linked elsewhere meanwhile. Start again from the install.",
        ));
    }
    audit(
        &state,
        &install_id,
        "link_jira",
        json!({ "team_id": link.team_id, "site_url": link.site_url, "by": link.linked_by }),
    )
    .await;
    Ok(Json(
        json!({ "outcome": "linked", "return_url": with_outcome(&ret, "linked") }),
    ))
}

/// GET /api/hybrid/jira-link (team admins): where this team's Jira site is
/// linked, if anywhere.
pub async fn team_link(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    master(&state).ok_or_else(not_here)?;
    team_admin(&claims)?;
    let Some(install_id) = install_for_team(&state, &claims.team_id).await else {
        return Ok(Json(json!({ "linked": false })));
    };
    let install = load_install(&state, &install_id).await?;
    let link = jira_link(&state, &install_id).await;
    Ok(Json(json!({
        "linked": true,
        "install_address": install.as_ref().map(install_address),
        "linked_at": link.map(|l| l.linked_at),
    })))
}

/// DELETE /api/hybrid/jira-link (team admins): unlink this team's site from
/// whichever install it is linked to; its events are handled here again.
pub async fn team_unlink(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    master(&state).ok_or_else(not_here)?;
    team_admin(&claims)?;
    if !unlink_team(&state, &claims.team_id, &claims.sub).await {
        return Err(fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "store_unavailable",
            "Couldn't unlink just now. Try again in a minute.",
        ));
    }
    Ok(Json(json!({ "linked": false })))
}

// ── The relay ────────────────────────────────────────────────────────────

/// The event as forwarded: a Forge event without the site's Forge secret
/// (the install trusts the relay's signature, and never holds the secret);
/// anything else unchanged (pure; tested).
pub fn without_secret(body: &[u8]) -> Vec<u8> {
    match serde_json::from_slice::<Value>(body) {
        Ok(mut v) => {
            let had = v
                .get_mut("coderhelm")
                .and_then(Value::as_object_mut)
                .and_then(|o| o.remove("forge_secret"))
                .is_some();
            if had {
                serde_json::to_vec(&v).unwrap_or_default()
            } else {
                body.to_vec()
            }
        }
        Err(_) => body.to_vec(),
    }
}

/// The id the install refuses a second copy of, for a plain Jira webhook:
/// its body's digest (Jira resends the same body; a captured body sent
/// again is the same id whatever its headers say).
pub fn webhook_delivery_id(body: &[u8]) -> String {
    let d = sha2::Sha256::digest(body);
    format!("jira-{}", &hex::encode(d)[..40])
}

/// The relay's part of the hosted Jira intake, called once the event has
/// passed the team's own check (Forge secret or webhook signature): when
/// the team's site is linked to an install, forward the event there and
/// answer for it, so the intake stops and never handles it too. None when
/// the site isn't linked, when the link is half gone (the stale route is
/// removed), or when the install is off: the intake then handles it as for
/// any hosted team.
///
/// `event` is `site` (a Forge event) or `site_webhook` (a plain Jira webhook).
pub async fn relay_team(
    state: &AppState,
    team_id: &str,
    event: &str,
    body: &[u8],
) -> Option<StatusCode> {
    let master = master(state)?;
    let install_id = install_for_team(state, team_id).await?;
    if jira_link(state, &install_id)
        .await
        .is_none_or(|l| l.team_id != team_id)
    {
        // A route without its link: remove it, and handle the event here.
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", s(&reverse_pk(team_id)))
            .key("sk", s("LINK"))
            .condition_expression("install_id = :i")
            .expression_attribute_values(":i", s(&install_id))
            .send()
            .await;
        return None;
    }
    let install = match load_install(state, &install_id).await {
        Ok(Some(i)) if i.verified() => i,
        _ => return None,
    };
    let body = without_secret(body);
    let mut d = Delivery::new(
        Direction::Outbound,
        relay::source_for("jira"),
        &format!("jira.{event}"),
        &install_id,
    );
    d.payload_bytes = body.len();
    d.redeliver = Some(deliveries::Redeliver::Relay);
    let delivery_id = if event == "site_webhook" {
        let id = webhook_delivery_id(&body);
        d.external_id = id.clone();
        id
    } else {
        d.id.clone()
    };
    forward_and_record(
        state,
        master,
        &install,
        d,
        "jira",
        event,
        &delivery_id,
        &body,
    )
    .await;
    // Jira and Forge hear 200: the relay owns the retries now.
    Some(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install(id: &str, verified: bool) -> Install {
        Install {
            id: id.into(),
            status: "active".into(),
            cloudfront: "d111122223333.cloudfront.net".into(),
            verified_at: if verified {
                "2026-10-01T00:00:00Z".into()
            } else {
                String::new()
            },
            ..Default::default()
        }
    }

    fn connected() -> TeamJira {
        TeamJira {
            site_url: "https://example.atlassian.net".into(),
            forge: true,
        }
    }

    const SITE: &str = "example.atlassian.net";

    #[test]
    fn only_the_site_the_install_asked_for_links_to_a_verified_install() {
        let i = install("ins_a", true);
        let t = connected();
        assert_eq!(link_blocked(Some(&i), &t, SITE, None, None, "TEAM#1"), None);
        assert_eq!(
            link_blocked(
                Some(&i),
                &t,
                "https://EXAMPLE.atlassian.net/",
                None,
                None,
                "TEAM#1"
            ),
            None
        );
        // Relinking the same site to the same install is fine.
        let same = JiraLink {
            team_id: "TEAM#1".into(),
            ..Default::default()
        };
        assert_eq!(
            link_blocked(Some(&i), &t, SITE, Some("ins_a"), Some(&same), "TEAM#1"),
            None
        );
        assert_eq!(
            link_blocked(
                Some(&install("ins_a", false)),
                &t,
                SITE,
                None,
                None,
                "TEAM#1"
            ),
            Some("install_off")
        );
        assert_eq!(
            link_blocked(None, &t, SITE, None, None, "TEAM#1"),
            Some("install_off")
        );
        let mut revoked = i.clone();
        revoked.status = "revoked".into();
        assert_eq!(
            link_blocked(Some(&revoked), &t, SITE, None, None, "TEAM#1"),
            Some("install_off")
        );
        // Ownership: only a team running CoderHelm's Jira app on a site; a
        // webhook token alone proves nothing.
        assert_eq!(
            link_blocked(Some(&i), &TeamJira::default(), SITE, None, None, "TEAM#1"),
            Some("jira_not_connected")
        );
        // A state someone else got hold of can't pair another team's site.
        assert_eq!(
            link_blocked(Some(&i), &t, "other.atlassian.net", None, None, "TEAM#1"),
            Some("site_mismatch")
        );
        assert_eq!(
            link_blocked(Some(&i), &t, "", None, None, "TEAM#1"),
            Some("site_mismatch")
        );
        // One install per site, one site per install.
        assert_eq!(
            link_blocked(Some(&i), &t, SITE, Some("ins_b"), None, "TEAM#1"),
            Some("linked_elsewhere")
        );
        let other = JiraLink {
            team_id: "TEAM#2".into(),
            ..Default::default()
        };
        assert_eq!(
            link_blocked(Some(&i), &t, SITE, None, Some(&other), "TEAM#1"),
            Some("install_has_other_site")
        );
        for code in [
            "install_off",
            "jira_not_connected",
            "site_mismatch",
            "linked_elsewhere",
            "install_has_other_site",
            "expired",
        ] {
            assert_ne!(blocked_message(code), blocked_message("nonsense"), "{code}");
        }
    }

    #[test]
    fn site_hosts() {
        assert_eq!(
            site_host("https://Example.atlassian.net/jira").as_deref(),
            Some(SITE)
        );
        assert_eq!(site_host(" example.atlassian.net ").as_deref(), Some(SITE));
        assert_eq!(site_host(""), None);
        assert_eq!(site_host("localhost"), None);
        assert_eq!(site_host("exa mple.net"), None);
        assert_eq!(site_host("a..b"), None);
    }

    #[test]
    fn link_and_unlink_write_both_records_together_and_never_over_another() {
        let link = JiraLink {
            team_id: "TEAM#1".into(),
            site_url: "https://example.atlassian.net".into(),
            linked_by: "user-1".into(),
            linked_at: "2026-10-30T00:00:00Z".into(),
        };
        let w = link_writes("settings", "ins_a", &link).unwrap();
        assert_eq!(w.len(), 2);
        let own = w[0].put().unwrap();
        assert_eq!(own.item()["pk"].as_s().unwrap(), "HYBRID#ins_a");
        assert_eq!(own.item()["sk"].as_s().unwrap(), LINK_SK);
        assert!(own.condition_expression().unwrap().contains("team_id = :t"));
        let rev = w[1].put().unwrap();
        assert_eq!(rev.item()["pk"].as_s().unwrap(), "HYBRIDJIRA#TEAM#1");
        assert_eq!(rev.item()["install_id"].as_s().unwrap(), "ins_a");
        assert!(rev
            .condition_expression()
            .unwrap()
            .contains("install_id = :i"));
        assert!(!own.item().contains_key("forge_secret"));
        let u = unlink_writes("settings", "ins_a", "TEAM#1").unwrap();
        assert_eq!(u.len(), 2);
        assert!(u[0]
            .delete()
            .unwrap()
            .condition_expression()
            .unwrap()
            .contains("team_id = :t"));
        assert!(u[1]
            .delete()
            .unwrap()
            .condition_expression()
            .unwrap()
            .contains("install_id = :i"));
        // The install never sees who linked it.
        let v = serde_json::to_value(&link).unwrap();
        assert!(v.get("linked_by").is_none());
    }

    #[test]
    fn relayed_events_never_carry_the_sites_secret() {
        let body = br#"{"issue":{"key":"ABC-1"},"coderhelm":{"team_id":"TEAM#1","forge_secret":"s3cret"}}"#;
        let out = without_secret(body);
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert!(v["coderhelm"].get("forge_secret").is_none());
        assert_eq!(v["issue"]["key"], "ABC-1");
        assert!(!String::from_utf8_lossy(&out).contains("s3cret"));
        // Nothing to strip: the body is unchanged byte for byte.
        let plain = br#"{"webhookEvent":"jira:issue_updated"}"#;
        assert_eq!(without_secret(plain), plain.to_vec());
        assert_eq!(without_secret(b"not json"), b"not json".to_vec());
        // The same webhook body is the same id; another body another id.
        assert_eq!(webhook_delivery_id(plain), webhook_delivery_id(plain));
        assert_ne!(webhook_delivery_id(plain), webhook_delivery_id(body));
        assert!(webhook_delivery_id(plain).starts_with("jira-"));
    }

    #[test]
    fn the_proxy_holds_the_secret_and_each_link_its_own_token() {
        let m = "m".repeat(40);
        let a = proxy_url("https://api.example.com/", &m, "ins_a", "add_comment");
        assert!(a.starts_with("https://api.example.com/hybrid/jira/trigger/ins_a/add_comment/"));
        // A token is for one install and one trigger.
        assert_ne!(
            sign::jira_trigger_token(&m, "ins_a", "add_comment"),
            sign::jira_trigger_token(&m, "ins_b", "add_comment")
        );
        assert_ne!(
            sign::jira_trigger_token(&m, "ins_a", "add_comment"),
            sign::jira_trigger_token(&m, "ins_a", "create_ticket")
        );
        let out = with_site_secret(
            br#"{"forge_secret":"token-for-install","keys":["A-1"]}"#,
            "real",
        );
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            (v["forge_secret"].as_str(), v["keys"][0].as_str()),
            (Some("real"), Some("A-1"))
        );
        // A body without the field gains nothing.
        let plain = br#"{"issueKey":"A-1","comment":"hi"}"#;
        assert_eq!(with_site_secret(plain, "real"), plain.to_vec());
        assert!(callable("https://example.atlassian.net/x/1"));
        assert!(!callable("http://example.atlassian.net/x/1"));
        assert!(!callable(""));
        assert_eq!(TRIGGERS.len(), 6);
    }

    #[test]
    fn states_and_return_urls() {
        assert!(is_link_state("hyj_abc-DEF_123"));
        assert!(!is_link_state("hyb_abc"));
        assert!(!is_link_state("hyj_a/b"));
        assert!(!is_link_state(&format!("hyj_{}", "a".repeat(60))));
        assert_eq!(
            with_outcome(
                "https://d1.cloudfront.net/settings/integrations/jira/#link",
                "linked"
            ),
            "https://d1.cloudfront.net/settings/integrations/jira/?hybrid_jira=linked#link"
        );
        assert_eq!(
            with_outcome(
                "https://d1.cloudfront.net/settings/integrations/jira/?a=1",
                "declined"
            ),
            "https://d1.cloudfront.net/settings/integrations/jira/?a=1&hybrid_jira=declined"
        );
        assert_eq!(
            install_address(&install("ins_a", true)),
            "d111122223333.cloudfront.net"
        );
    }
}
