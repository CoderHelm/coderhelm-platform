//! The built-in bases on the Knowledge page, and share links (`crate::bases`
//! decides who reads what):
//!
//! - `GET /api/knowledge/builtin` — the six bases: what each holds, counts,
//!   when it last changed, its audience and whether it is open to the asker.
//!   Any member; a closed base shows its name and why, never its contents.
//! - `PUT /api/knowledge/builtin/{key}` — a base's audience: roles, people,
//!   readers from AI tools, paused. Admins.
//! - `PUT /api/knowledge/readers` — whether people may connect AI tools as
//!   readers without an account (never a seat). Admins.
//! - `GET /api/knowledge/builtin/{key}/search?q=` — searches one base as the
//!   asker, exactly as an AI tool's `search` does.
//! - `POST /api/share`, `GET`/`DELETE /api/share/{id}` — share links. A link
//!   holds the search (base and words), never what it found: every open
//!   searches again as the person opening it, so a link never carries the
//!   sharer's access. Links end after 1, 7 or 30 days, and the sharer or an
//!   admin can end one sooner.

use std::collections::BTreeSet;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Path, Query, State};
use axum::{http::StatusCode, Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::error;

use crate::bases::{self, Asker, Audience};
use crate::lookups;
use crate::models::Claims;
use crate::routes::roles::{self, AuditEntry};
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

fn internal(context: &str, e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong. Try again.",
    )
}

/// The scopes a dashboard read holds: every base, as Chat reads them. The
/// role and the audiences still decide.
pub fn dashboard_scopes() -> BTreeSet<&'static str> {
    crate::mcp_oauth::SCOPES.iter().copied().collect()
}

/// GET /api/knowledge/builtin
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let holder = roles::holder_for(&state, &claims).await;
    let who = Asker {
        holder: &holder,
        email: &claims.email,
    };
    let bases = bases::list(&state, &claims.team_id, &who, None).await;
    Ok(Json(json!({
        "bases": bases,
        "readers": { "enabled": bases::readers_enabled(&state, &claims.team_id).await },
        "can_manage": claims.support_view.is_none() && holder.holds("knowledge_manage"),
    })))
}

#[derive(Deserialize, Default)]
pub struct AudienceInput {
    pub roles: Option<Vec<String>>,
    pub people: Option<Vec<String>>,
    pub readers: Option<bool>,
    pub paused: Option<bool>,
}

/// The audience after an edit: only what the edit names changes.
pub fn edited(mut a: Audience, input: &AudienceInput) -> Result<Audience, String> {
    if let Some(r) = &input.roles {
        a.roles = r.clone();
    }
    if let Some(p) = &input.people {
        a.people = p.clone();
    }
    if let Some(r) = input.readers {
        a.readers = r;
    }
    if let Some(p) = input.paused {
        a.paused = p;
    }
    a.checked()
}

/// PUT /api/knowledge/builtin/{key}
pub async fn update(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(key): Path<String>,
    Json(input): Json<AudienceInput>,
) -> Reply {
    claims.require_role(3).map_err(|_| {
        fail(
            StatusCode::FORBIDDEN,
            "Only admins choose who reads a base.",
        )
    })?;
    let base = lookups::base(&key).ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such base."))?;
    let mut all = bases::audiences(&state, &claims.team_id).await;
    let current = all
        .remove(base.key)
        .unwrap_or_else(|| bases::default_audience(base.key));
    let next = edited(current, &input).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "base_audience",
            role_id: base.key,
            role_name: base.name,
            detail: format!("Set who reads {}: {}.", base.name, next.label()),
        },
    )
    .await?;
    bases::save_audience(&state, &claims.team_id, base.key, &next, &claims.email)
        .await
        .map_err(|e| internal("base audience not saved", e))?;
    let holder = roles::holder_for(&state, &claims).await;
    let who = Asker {
        holder: &holder,
        email: &claims.email,
    };
    let d = bases::decide(base, &who, &next, None);
    let size = if d.is_open() {
        bases::size(&state, &claims.team_id, base.key).await
    } else {
        bases::Size::default()
    };
    Ok(Json(bases::view(base, &size, &next, &d)))
}

#[derive(Deserialize)]
pub struct ReadersInput {
    pub enabled: bool,
}

/// PUT /api/knowledge/readers
pub async fn set_readers(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(input): Json<ReadersInput>,
) -> Reply {
    claims.require_role(3).map_err(|_| {
        fail(
            StatusCode::FORBIDDEN,
            "Only admins choose whether people connect as readers.",
        )
    })?;
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "readers",
            role_id: bases::READER_ROLE,
            role_name: "Readers",
            detail: if input.enabled {
                "Let people in the team's GitHub organization connect AI tools as readers, without an account or a seat.".into()
            } else {
                "Stopped new readers from connecting AI tools.".into()
            },
        },
    )
    .await?;
    bases::set_readers(&state, &claims.team_id, input.enabled, &claims.email)
        .await
        .map_err(|e| internal("readers setting not saved", e))?;
    Ok(Json(json!({ "enabled": input.enabled })))
}

#[derive(Deserialize)]
pub struct SearchQuery {
    #[serde(default)]
    pub q: String,
}

/// Searches one base as this person, through the same lookups AI tools use.
pub async fn search_as(
    state: &Arc<AppState>,
    claims: &Claims,
    key: &str,
    query: &str,
) -> Result<Value, (StatusCode, Json<Value>)> {
    let base = lookups::base(key).ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such base."))?;
    let holder = roles::holder_for(state, claims).await;
    let env = crate::mcp_lookups::Env {
        state: state.clone(),
        claims: claims.clone(),
        holder,
        scopes: dashboard_scopes(),
        grant_id: "dashboard".into(),
    };
    match Box::pin(crate::mcp_lookups::run(
        &env,
        crate::mcp_lookups::SEARCH,
        &json!({ "query": query, "base": base.key }),
    ))
    .await
    {
        Ok(a) => Ok(a.value),
        Err(crate::mcp_lookups::Fail::Scope(_)) => Err(fail(
            StatusCode::FORBIDDEN,
            &format!("{} isn't open to you.", base.name),
        )),
        Err(crate::mcp_lookups::Fail::Message(m)) => {
            let status = if m.contains("isn't open to you") || m.contains("paused") {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::BAD_REQUEST
            };
            Err(fail(status, &m))
        }
    }
}

/// GET /api/knowledge/builtin/{key}/search?q=
pub async fn search(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(key): Path<String>,
    Query(q): Query<SearchQuery>,
) -> Reply {
    search_as(&state, &claims, &key, &q.q).await.map(Json)
}

// ── Share links ─────────────────────────────────────────────────────────────

pub const SHARE_PREFIX: &str = "SHARE#";
/// How long a link may live, in days.
pub const SHARE_DAYS: [i64; 3] = [1, 7, 30];

#[derive(Deserialize)]
pub struct ShareInput {
    pub base: String,
    pub query: String,
    #[serde(default)]
    pub days: Option<i64>,
}

/// A link's days, checked: 1, 7 (the default) or 30.
pub fn share_days(asked: Option<i64>) -> Result<i64, String> {
    match asked {
        None => Ok(7),
        Some(d) if SHARE_DAYS.contains(&d) => Ok(d),
        Some(_) => Err("A link lasts 1, 7 or 30 days.".into()),
    }
}

fn share_id() -> String {
    use rand::RngCore;
    let mut b = [0u8; 16];
    rand::rng().fill_bytes(&mut b);
    hex::encode(b)
}

fn s(v: impl Into<String>) -> AttributeValue {
    AttributeValue::S(v.into())
}

/// POST /api/share
pub async fn create_share(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(input): Json<ShareInput>,
) -> Reply {
    let base =
        lookups::base(&input.base).ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such base."))?;
    let query = input.query.trim();
    if query.is_empty() || query.chars().count() > 1_000 {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Share a search of 1 to 1,000 characters.",
        ));
    }
    let days = share_days(input.days).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    // Only what the sharer can read may be shared.
    let holder = roles::holder_for(&state, &claims).await;
    let auds = bases::audiences(&state, &claims.team_id).await;
    let a = auds
        .get(base.key)
        .cloned()
        .unwrap_or_else(|| bases::default_audience(base.key));
    let who = Asker {
        holder: &holder,
        email: &claims.email,
    };
    if let Some(why) = bases::decide(base, &who, &a, None).why(base) {
        return Err(fail(StatusCode::FORBIDDEN, &why));
    }
    let id = share_id();
    let now = chrono::Utc::now();
    let expires = now + chrono::Duration::days(days);
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(&claims.team_id))
        .item("sk", s(format!("{SHARE_PREFIX}{id}")))
        .item("base", s(base.key))
        .item("query", s(query))
        .item("created_by", s(&claims.email))
        .item("created_by_sub", s(&claims.sub))
        .item("created_at", s(now.to_rfc3339()))
        .item("expires_at", s(expires.to_rfc3339()))
        .item("ttl", AttributeValue::N(expires.timestamp().to_string()))
        .send()
        .await
        .map_err(|e| internal("share link not saved", e))?;
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "share_created",
            role_id: &id,
            role_name: base.name,
            detail: format!("Shared a search of {} for {days} days.", base.name),
        },
    )
    .await?;
    Ok(Json(json!({
        "id": id,
        "path": format!("/s/?id={id}"),
        "expires_at": expires.to_rfc3339(),
    })))
}

async fn load_share(
    state: &AppState,
    team_id: &str,
    id: &str,
) -> Result<std::collections::HashMap<String, AttributeValue>, (StatusCode, Json<Value>)> {
    let gone = || {
        fail(
            StatusCode::NOT_FOUND,
            "This link doesn't exist or has ended.",
        )
    };
    if id.is_empty() || id.len() > 64 || !id.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(gone());
    }
    // Only the asker's own team: another team's link is not found.
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(team_id))
        .key("sk", s(format!("{SHARE_PREFIX}{id}")))
        .send()
        .await
        .map_err(|e| internal("share link not read", e))?
        .item
        .ok_or_else(gone)?;
    let expires = item
        .get("expires_at")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    if share_expired(&expires, chrono::Utc::now()) {
        return Err(gone());
    }
    Ok(item)
}

/// Whether a link has ended (an unreadable time has).
pub fn share_expired(expires_at: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
    chrono::DateTime::parse_from_rfc3339(expires_at).map_or(true, |t| t <= now)
}

/// GET /api/share/{id} — searches again as the person opening it.
pub async fn get_share(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let item = load_share(&state, &claims.team_id, &id).await?;
    let get = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let key = get("base");
    let base = lookups::base(&key).ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such base."))?;
    let query = get("query");
    let mut out = json!({
        "id": id,
        "base": base.key,
        "base_name": base.name,
        "query": query,
        "created_by": get("created_by"),
        "expires_at": get("expires_at"),
    });
    match search_as(&state, &claims, base.key, &query).await {
        Ok(v) => {
            out["open"] = json!(true);
            out["why"] = Value::Null;
            out["results"] = v["results"].clone();
            out["withheld"] = v["withheld"].clone();
        }
        Err((StatusCode::FORBIDDEN, _)) => {
            out["open"] = json!(false);
            out["why"] = json!(format!(
                "This was shared with you, but {} isn't open to you. Ask an admin for access.",
                base.name
            ));
            out["results"] = json!([]);
        }
        Err(e) => return Err(e),
    }
    Ok(Json(out))
}

/// DELETE /api/share/{id} — the sharer, or an admin.
pub async fn delete_share(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let item = load_share(&state, &claims.team_id, &id).await?;
    let by = item
        .get("created_by_sub")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    if by != claims.sub && claims.role_level() < 3 {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only the person who shared it or an admin can end a link.",
        ));
    }
    state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&claims.team_id))
        .key("sk", s(format!("{SHARE_PREFIX}{id}")))
        .send()
        .await
        .map_err(|e| internal("share link not ended", e))?;
    Ok(Json(json!({ "ok": true })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_edit_changes_only_what_it_names() {
        let a = bases::default_audience(lookups::INFRA);
        let e = edited(
            a.clone(),
            &AudienceInput {
                roles: Some(vec!["member".into()]),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(e.roles, ["member"]);
        assert_eq!((e.readers, e.paused), (a.readers, a.paused));
        assert!(edited(
            a,
            &AudienceInput {
                roles: Some(vec!["nobody".into()]),
                ..Default::default()
            }
        )
        .is_err());
    }

    #[test]
    fn links_last_one_seven_or_thirty_days() {
        assert_eq!(share_days(None), Ok(7));
        for d in SHARE_DAYS {
            assert_eq!(share_days(Some(d)), Ok(d));
        }
        assert!(share_days(Some(365)).is_err());
        let now = chrono::Utc::now();
        assert!(share_expired("not a time", now));
        assert!(share_expired(
            &(now - chrono::Duration::seconds(1)).to_rfc3339(),
            now
        ));
        assert!(!share_expired(
            &(now + chrono::Duration::days(1)).to_rfc3339(),
            now
        ));
        assert_eq!(share_id().len(), 32);
    }
}
