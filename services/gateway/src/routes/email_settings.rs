//! Settings, Emails (self-hosted installs; install admins only).
//!
//! `GET /api/system/emails` lists the optional emails with their on/off
//! state, the essential ones with why they are needed, and how the install
//! sends (its own Amazon SES or CoderHelm's mail relay). `PUT` changes them.
//! Turning the relay on lists the install's people with it (install admins,
//! members, pending invites) first; if that fails nothing changes.

use std::collections::BTreeMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{extract::State, http::StatusCode, Extension, Json};
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
    common::hybrid::client::call(&state.http, &creds, "GET", relay::PATH_STATUS, None)
        .await
        .map(|v| with_org_name(v, mail::naming::org_from_env().as_deref()))
        .map_err(|e| json!({ "error": e.code, "message": e.message }))
        .map_or_else(Some, Some)
}

/// The relay's status names the install by its address; the organization
/// this install's config names goes first when the relay would accept it
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
