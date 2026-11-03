//! Settings, Emails (self-hosted installs; install admins only).
//!
//! `GET /api/system/emails` lists the optional emails with their on/off
//! state, the essential ones with why they are needed, and how the install
//! sends (its own Amazon SES or CoderHelm's mail relay). `PUT` changes them.
//! Turning the relay on lists the install's people with it (install admins,
//! members, pending invites) first; if that fails nothing changes.
//!
//! `GET /api/system/emails/sender` shows the sender name and organization
//! the install's email uses and how its From line and subjects read; `PUT`
//! changes them ([`mail::sender_name`]).

use std::collections::BTreeMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{extract::State, http::StatusCode, Extension, Json};
use common::mail::sender_name::{self, SenderName};
use common::mail::{self, relay, Settings, Transport};
use serde_json::{json, Value};
use tracing::warn;

use super::api::is_platform_admin;
use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(code: StatusCode, reason: &str, message: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({ "code": reason, "error": message })))
}

fn status_only(s: StatusCode) -> (StatusCode, Json<Value>) {
    (s, Json(json!({})))
}

/// An install admin on a self-hosted install, or 403/404.
fn allowed(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    if !super::setup_checks::self_hosted() {
        return Err(status_only(StatusCode::NOT_FOUND));
    }
    if !is_platform_admin(claims) {
        return Err(status_only(StatusCode::FORBIDDEN));
    }
    Ok(())
}

/// The install's sender for `common::mail::send`.
pub fn sender(state: &AppState) -> mail::Sender<'_> {
    mail::Sender {
        ses: &state.ses,
        http: &state.http,
        dynamo: &state.dynamo,
        settings_table: &state.config.settings_table_name,
        from: &state.config.ses_from_address,
    }
}

/// What CoderHelm sees when the install sends through it, in plain words.
pub const TRUST_NOTE: &str = "CoderHelm sees each recipient's address, the template id and its parameters (names, PR titles and run links in the weekly summary), and keeps only delivery metadata after sending: the template id, a shortened address and whether it was delivered, for 3 days.";

/// The page's data. Pure apart from the inputs.
pub fn view(
    settings: Option<&Settings>,
    defaults: &[String],
    registered: bool,
    hybrid_off: bool,
    from: &str,
    relay_status: Option<Value>,
) -> Value {
    let s = settings.cloned().unwrap_or_default();
    let transport = mail::transport_of(settings, mail::transport_default());
    json!({
        "self_hosted": true,
        "optional": mail::OPTIONAL.iter().map(|o| json!({
            "id": o.id,
            "label": o.label,
            "description": o.description,
            "on": mail::optional_on(mail::Deployment::SelfHosted, settings, defaults, o.id),
        })).collect::<Vec<_>>(),
        "essential": mail::ESSENTIAL,
        "transport": transport.as_str(),
        "ses": {
            "from": from,
            "check": "email",
            "note": "Sending uses this install's own Amazon SES, which needs SES production access to email anyone outside your verified addresses.",
        },
        "relay": {
            "available": registered && !hybrid_off,
            "registered": registered,
            "turned_off": hybrid_off,
            "trust": TRUST_NOTE,
            "status": relay_status,
        },
        "updated_by": s.updated_by,
        "updated_at": s.updated_at,
    })
}

/// Checks a change. `Ok` holds the new settings.
pub fn apply(current: Option<&Settings>, body: &Value) -> Result<Settings, &'static str> {
    let Some(obj) = body.as_object() else {
        return Err("send an object");
    };
    if obj.keys().any(|k| k != "optional" && k != "transport") {
        return Err("unknown setting");
    }
    let mut s = current.cloned().unwrap_or_default();
    if let Some(o) = obj.get("optional") {
        let o = o
            .as_object()
            .ok_or("optional: an object of id to true or false")?;
        let mut next = s.optional.clone();
        for (k, v) in o {
            if !mail::OPTIONAL.iter().any(|x| x.id == k) {
                return Err("unknown optional email");
            }
            next.insert(k.clone(), v.as_bool().ok_or("optional: true or false")?);
        }
        s.optional = next;
    }
    if let Some(t) = obj.get("transport") {
        s.transport = t
            .as_str()
            .and_then(Transport::parse)
            .ok_or("transport: ses or coderhelm")?;
    }
    Ok(s)
}

async fn relay_status(state: &AppState) -> Option<Value> {
    let creds = common::hybrid::client::credentials()?;
    let stored = sender_name::load_for_send(&state.dynamo, &state.config.settings_table_name).await;
    let org = sender_name::relay_org(stored.as_ref(), mail::naming::org_from_env());
    common::hybrid::client::call(&state.http, &creds, "GET", relay::PATH_STATUS, None)
        .await
        .map(|v| with_org_name(v, org.as_deref()))
        .map_err(|e| json!({ "error": e.code, "message": e.message }))
        .map_or_else(Some, Some)
}

/// The relay's status names the install by its address; the organization
/// this install names (Settings, else its config) goes first when the relay would accept it
/// (the same rule it applies to each email).
fn with_org_name(mut status: Value, org: Option<&str>) -> Value {
    if let Some(label) = org.and_then(mail::naming::org_label) {
        if status.get("display_name").is_some() {
            status["display_name"] = json!(mail::naming::display_name(Some(&label)));
        }
    }
    status
}

/// GET /api/system/emails
pub async fn get(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(status_only)?;
    allowed(&claims)?;
    let settings = mail::load(&state.dynamo, &state.config.settings_table_name)
        .await
        .map_err(|e| {
            warn!(error = %e, "email settings read failed");
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?;
    let registered = common::hybrid::client::credentials().is_some();
    let status = if mail::transport_of(settings.as_ref(), mail::transport_default())
        == Transport::Coderhelm
    {
        relay_status(&state).await
    } else {
        None
    };
    Ok(Json(view(
        settings.as_ref(),
        &mail::config_defaults(),
        registered,
        common::hybrid::turned_off(),
        &state.config.ses_from_address,
        status,
    )))
}

/// Everyone the install may email: install admins, members, pending invites.
async fn people(state: &AppState, actor: &str) -> Vec<relay::NewRecipient> {
    let mut out: BTreeMap<String, relay::RecipientKind> = BTreeMap::new();
    let mut start = None;
    for _ in 0..50 {
        let r = state
            .dynamo
            .scan()
            .table_name(&state.config.users_table_name)
            .filter_expression("begins_with(sk, :u)")
            .expression_attribute_values(":u", AttributeValue::S("USER#".into()))
            .projection_expression("email, #st")
            .expression_attribute_names("#st", "status")
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(r) = r else { break };
        for i in r.items() {
            let Some(e) = i.get("email").and_then(|v| v.as_s().ok()) else {
                continue;
            };
            if !relay::valid_address(e) || relay::is_coderhelm_address(e) {
                continue;
            }
            let invited = i
                .get("status")
                .and_then(|v| v.as_s().ok())
                .map(String::as_str)
                == Some("invited");
            out.entry(relay::normalize(e)).or_insert(if invited {
                relay::RecipientKind::Invite
            } else {
                relay::RecipientKind::Member
            });
        }
        match r.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    let admins = std::env::var("PLATFORM_ADMIN_EMAILS").unwrap_or_default();
    for a in admins.split(',').map(str::trim).chain([actor]) {
        if relay::valid_address(a) && !relay::is_coderhelm_address(a) {
            out.insert(relay::normalize(a), relay::RecipientKind::Admin);
        }
    }
    // The admin who turns it on comes first (the relay's first listing).
    let actor = relay::normalize(actor);
    let mut list: Vec<relay::NewRecipient> = out
        .into_iter()
        .map(|(email, kind)| relay::NewRecipient { email, kind })
        .collect();
    list.sort_by_key(|r| (r.email != actor, r.kind != relay::RecipientKind::Admin));
    list.truncate(relay::BOOTSTRAP_RECIPIENTS);
    list
}

/// PUT /api/system/emails `{optional?: {id: bool}, transport?: "ses"|"coderhelm"}`
pub async fn put(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(status_only)?;
    allowed(&claims)?;
    let table = &state.config.settings_table_name;
    let current = mail::load(&state.dynamo, table).await.map_err(|_| {
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "store_unavailable",
            "Try again in a minute.",
        )
    })?;
    let was = mail::transport_of(current.as_ref(), mail::transport_default());
    let base = current.clone().unwrap_or(Settings {
        transport: was,
        ..Default::default()
    });
    let mut next =
        apply(Some(&base), &body).map_err(|m| fail(StatusCode::BAD_REQUEST, "bad_request", m))?;
    if next.transport == Transport::Coderhelm && was != Transport::Coderhelm {
        if common::hybrid::turned_off() {
            return Err(fail(
                StatusCode::CONFLICT,
                "relay_turned_off",
                "This install's config turned hybrid off.",
            ));
        }
        let Some(creds) = common::hybrid::client::credentials() else {
            return Err(fail(
                StatusCode::CONFLICT,
                "relay_not_registered",
                "Register this install with CoderHelm first (Settings, System, Hybrid).",
            ));
        };
        let list = people(&state, &claims.email).await;
        mail::list(&state.http, &creds, &claims.email, &list)
            .await
            .map_err(|e| fail(StatusCode::BAD_GATEWAY, &e.code, &e.message))?;
    }
    next.updated_by = claims.email.clone();
    next.updated_at = chrono::Utc::now().to_rfc3339();
    mail::save(&state.dynamo, table, &next).await.map_err(|e| {
        warn!(error = %e, "email settings write failed");
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "store_unavailable",
            "Try again in a minute.",
        )
    })?;
    Ok(Json(view(
        Some(&next),
        &mail::config_defaults(),
        common::hybrid::client::credentials().is_some(),
        common::hybrid::turned_off(),
        &state.config.ses_from_address,
        None,
    )))
}

/// How the From name and a subject read on each route with `stored` (none:
/// the config's names alone).
struct Names {
    ses_name: String,
    ses_prefix: String,
    relay_name: String,
    relay_prefix: String,
    /// The relay would use the organization (it passes `org_label`).
    org_used: bool,
}

fn names(stored: Option<&SenderName>, app_host: &str, relay_display: Option<&str>) -> Names {
    use mail::naming;
    let zone = naming::coderhelm_zone();
    let ses_name = sender_name::ses_name(
        stored,
        std::env::var("EMAIL_FROM_NAME").ok().as_deref(),
        std::env::var("ORGANIZATION_NAME").ok().as_deref(),
        app_host,
        &zone,
    );
    let org = sender_name::relay_org(stored, naming::org_from_env());
    // Update notices from the install's own SES start with its organization
    // or its address (lambda/updater, notice_label).
    let ses_prefix =
        naming::subject_prefix(naming::ses_label(org.as_deref(), app_host, &zone).as_deref());
    // The relay's own name for the install, when the organization isn't
    // one it would use: from its status, else the install's address.
    let fallback = relay_display
        .map(str::to_string)
        .unwrap_or_else(|| naming::display_name(naming::host_label(app_host, &zone).as_deref()));
    let label = org.as_deref().and_then(naming::org_label);
    let relay_name = label
        .as_deref()
        .map(|l| naming::display_name(Some(l)))
        .unwrap_or(fallback);
    let relay_prefix = naming::subject_prefix(
        relay_name
            .strip_suffix(naming::PRODUCT)
            .map(str::trim)
            .filter(|l| !l.is_empty()),
    );
    Names {
        ses_name,
        ses_prefix,
        relay_name,
        relay_prefix,
        org_used: label.is_some(),
    }
}

/// What the sender name page shows: the stored values, and how the From
/// line and a subject read with them on each route, and with nothing stored
/// (`fallback`, for a preview while the fields are cleared). Pure apart from
/// the inputs. `relay_from` and `relay_display` come from the relay's status
/// (its address, and the name it gives the install with no organization).
pub fn sender_view(
    stored: Option<&SenderName>,
    transport: Transport,
    from: &str,
    app_host: &str,
    relay_from: Option<&str>,
    relay_display: Option<&str>,
) -> Value {
    let s = stored.cloned().unwrap_or_default();
    let now = names(stored, app_host, relay_display);
    let none = names(None, app_host, relay_display);
    json!({
        "sender_name": s.sender_name,
        "organization": s.organization,
        "max": { "sender_name": sender_name::SENDER_MAX, "organization": mail::naming::ORG_MAX },
        // What every name through the relay ends with.
        "suffix": mail::naming::PRODUCT,
        "transport": transport.as_str(),
        "ses": {
            "from_address": from,
            "display_name": now.ses_name,
            "from": mail::naming::from_header(&now.ses_name, from),
            "subject_prefix": now.ses_prefix,
        },
        "relay": {
            "from_address": relay_from,
            "display_name": now.relay_name,
            "subject_prefix": now.relay_prefix,
            "organization_used": now.org_used,
        },
        "fallback": {
            "ses_display_name": none.ses_name,
            "ses_subject_prefix": none.ses_prefix,
            "relay_display_name": none.relay_name,
            "relay_subject_prefix": none.relay_prefix,
        },
        "updated_by": s.updated_by,
        "updated_at": s.updated_at,
    })
}

/// Checks a sender name change. `Ok` holds the new values.
pub fn apply_sender(
    current: Option<&SenderName>,
    body: &Value,
) -> Result<SenderName, &'static str> {
    let Some(obj) = body.as_object() else {
        return Err("send an object");
    };
    if obj
        .keys()
        .any(|k| k != "sender_name" && k != "organization")
    {
        return Err("unknown setting");
    }
    let mut s = current.cloned().unwrap_or_default();
    if let Some(v) = obj.get("sender_name") {
        let v = v.as_str().ok_or("sender_name: a string")?;
        s.sender_name = sender_name::clean_sender_name(v)?;
    }
    if let Some(v) = obj.get("organization") {
        let v = v.as_str().ok_or("organization: a string")?;
        s.organization = sender_name::clean_organization(v)?;
    }
    Ok(s)
}

async fn sender_page(
    state: &AppState,
    stored: Option<&SenderName>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    let settings = mail::load(&state.dynamo, &state.config.settings_table_name)
        .await
        .map_err(|_| {
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?;
    let transport = mail::transport_of(settings.as_ref(), mail::transport_default());
    let status = if transport == Transport::Coderhelm {
        match common::hybrid::client::credentials() {
            Some(creds) => {
                common::hybrid::client::call(&state.http, &creds, "GET", relay::PATH_STATUS, None)
                    .await
                    .ok()
            }
            None => None,
        }
    } else {
        None
    };
    let text = |k: &str| {
        status
            .as_ref()
            .and_then(|s| s.get(k))
            .and_then(Value::as_str)
    };
    let app = common::public_urls::app_url_from_env();
    Ok(sender_view(
        stored,
        transport,
        &state.config.ses_from_address,
        common::public_urls::host_of(app),
        text("from"),
        text("display_name"),
    ))
}

/// GET /api/system/emails/sender
pub async fn get_sender(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(status_only)?;
    allowed(&claims)?;
    let stored = sender_name::load(&state.dynamo, &state.config.settings_table_name)
        .await
        .map_err(|e| {
            warn!(error = %e, "email sender name read failed");
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?;
    Ok(Json(sender_page(&state, stored.as_ref()).await?))
}

/// PUT /api/system/emails/sender `{sender_name?: string, organization?: string}`
/// (an empty string clears one, and the config's name applies again)
pub async fn put_sender(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(status_only)?;
    allowed(&claims)?;
    let table = &state.config.settings_table_name;
    let current = sender_name::load(&state.dynamo, table).await.map_err(|_| {
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "store_unavailable",
            "Try again in a minute.",
        )
    })?;
    let mut next = apply_sender(current.as_ref(), &body)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, "bad_request", m))?;
    next.updated_by = claims.email.clone();
    next.updated_at = chrono::Utc::now().to_rfc3339();
    sender_name::save(&state.dynamo, table, &next)
        .await
        .map_err(|e| {
            warn!(error = %e, "email sender name write failed");
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "store_unavailable",
                "Try again in a minute.",
            )
        })?;
    Ok(Json(sender_page(&state, Some(&next)).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_emails_start_off_and_the_install_sends_through_its_own_ses() {
        let v = view(None, &[], false, false, "ch@example.com", None);
        for o in v["optional"].as_array().unwrap() {
            assert_eq!(o["on"], false, "{}", o["id"]);
            assert!(!o["description"].as_str().unwrap().is_empty());
        }
        assert_eq!(v["transport"], "ses");
        assert_eq!(v["relay"]["available"], false);
        assert!(v["essential"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| !e["why"].as_str().unwrap().is_empty()));
        assert!(v["relay"]["trust"]
            .as_str()
            .unwrap()
            .contains("template id"));
    }

    #[test]
    fn changes_are_checked() {
        let s = apply(None, &json!({"optional": {"weekly_summary": true}})).unwrap();
        assert_eq!(s.optional.get("weekly_summary"), Some(&true));
        assert_eq!(s.transport, Transport::Ses);
        let s = apply(Some(&s), &json!({"transport": "coderhelm"})).unwrap();
        assert_eq!(s.transport, Transport::Coderhelm);
        assert_eq!(s.optional.get("weekly_summary"), Some(&true));
        assert!(apply(None, &json!({"optional": {"marketing": true}})).is_err());
        assert!(apply(None, &json!({"optional": {"welcome": "yes"}})).is_err());
        assert!(apply(None, &json!({"transport": "smtp"})).is_err());
        assert!(apply(None, &json!({"from": "x@y.com"})).is_err());
    }

    fn claims(role: &str, email: &str) -> Claims {
        Claims {
            sub: "USER#me".into(),
            team_id: "TEAM#a".into(),
            email: email.into(),
            role: role.into(),
            github_login: None,
            aud: "coderhelm-dashboard".into(),
            exp: 0,
            iat: 0,
            support_view: None,
            custom: None,
            api_key: None,
        }
    }

    #[test]
    fn only_install_admins_reach_the_sender_name() {
        // Both handlers check the role and the install admin list before
        // they read or write anything.
        let src = include_str!("email_settings.rs");
        for f in ["pub async fn get_sender(", "pub async fn put_sender("] {
            let body = src.split(f).nth(1).unwrap();
            let body = &body[..body.find("\n}\n").unwrap()];
            let role = body.find("claims.require_role(3)").expect(f);
            let admin = body.find("allowed(&claims)?").expect(f);
            let store = body.find("sender_name::").expect(f);
            assert!(role < store && admin < store, "{f}");
        }
        for role in ["viewer", "member", "billing"] {
            assert!(
                claims(role, "me@example.com").require_role(3).is_err(),
                "{role}"
            );
        }
        // A team admin who isn't an install admin is refused too.
        assert!(allowed(&claims("admin", "someone@example.com")).is_err());
        let cap = crate::permissions::handler_capability("email_settings::put_sender").unwrap();
        assert_eq!(cap.key, "emails");
        assert_eq!(cap.min_role, "admin");
    }

    #[test]
    fn sender_name_changes_are_checked() {
        let s = apply_sender(None, &json!({"sender_name": "  Acme   Robots "})).unwrap();
        assert_eq!(s.sender_name, "Acme Robots");
        assert_eq!(s.organization, "");
        let s = apply_sender(Some(&s), &json!({"organization": "Acme"})).unwrap();
        assert_eq!(
            (s.sender_name.as_str(), s.organization.as_str()),
            ("Acme Robots", "Acme")
        );
        // Empty clears one and keeps the other.
        let s = apply_sender(Some(&s), &json!({"sender_name": ""})).unwrap();
        assert_eq!(
            (s.sender_name.as_str(), s.organization.as_str()),
            ("", "Acme")
        );
        for bad in [
            json!({"sender_name": "Acme\r\nBcc: x@example.com"}),
            json!({"sender_name": "\"Acme\" <x@example.com>"}),
            json!({"sender_name": "x".repeat(65)}),
            json!({"sender_name": "d1.cloudfront.net"}),
            json!({"sender_name": 5}),
            json!({"organization": "CoderHelm Support"}),
            json!({"organization": "Billing"}),
            json!({"from": "x@example.com"}),
            json!("Acme"),
        ] {
            assert!(apply_sender(None, &bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_sender_page_shows_how_each_route_reads() {
        let stored = SenderName {
            sender_name: "Acme Robots".into(),
            organization: "Acme".into(),
            ..Default::default()
        };
        let v = sender_view(
            Some(&stored),
            Transport::Coderhelm,
            "ch@example.com",
            "acme.coderhelm.com",
            Some("notifications@notify.coderhelm.com"),
            Some("acme CoderHelm"),
        );
        assert_eq!(v["ses"]["display_name"], "Acme Robots");
        assert_eq!(v["ses"]["from"], "\"Acme Robots\" <ch@example.com>");
        assert_eq!(v["relay"]["display_name"], "Acme CoderHelm");
        assert_eq!(v["relay"]["subject_prefix"], "Acme CoderHelm: ");
        assert_eq!(v["relay"]["organization_used"], true);
        assert_eq!(v["ses"]["subject_prefix"], "Acme CoderHelm: ");
        assert_eq!(v["transport"], "coderhelm");
        // With nothing stored: the relay's own name for the install.
        assert_eq!(v["fallback"]["relay_display_name"], "acme CoderHelm");
        assert_eq!(v["fallback"]["relay_subject_prefix"], "acme CoderHelm: ");
        // A stored sender name that reads as CoderHelm's own mail isn't
        // the relay's label: the relay's own name for the install stays.
        let stored = SenderName {
            sender_name: "CoderHelm Support".into(),
            ..Default::default()
        };
        let v = sender_view(
            Some(&stored),
            Transport::Coderhelm,
            "ch@example.com",
            "acme.coderhelm.com",
            None,
            Some("acme CoderHelm"),
        );
        assert_eq!(v["relay"]["display_name"], "acme CoderHelm");
        assert_eq!(v["relay"]["subject_prefix"], "acme CoderHelm: ");
        assert_eq!(v["relay"]["organization_used"], false);
    }

    #[test]
    fn the_trust_note_says_what_coderhelm_sees() {
        for w in ["address", "template id", "parameters", "metadata"] {
            assert!(TRUST_NOTE.contains(w), "{w}");
        }
    }

    #[test]
    fn the_relay_status_shows_the_organizations_sender_name() {
        let status = json!({"display_name": "acme CoderHelm", "sent_today": 1});
        assert_eq!(
            super::with_org_name(status.clone(), Some("Acme"))["display_name"],
            "Acme CoderHelm"
        );
        // A name the relay would refuse isn't shown either.
        assert_eq!(
            super::with_org_name(status.clone(), Some("CoderHelm Support"))["display_name"],
            "acme CoderHelm"
        );
        assert_eq!(
            super::with_org_name(status, None)["display_name"],
            "acme CoderHelm"
        );
    }
}
