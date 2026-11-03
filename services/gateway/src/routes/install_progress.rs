//! The install guide's progress, kept on hosted CoderHelm so nothing in a
//! self-hosted install's AWS account is ever public.
//!
//! 1. The guide makes a random progress id and key in the browser and
//!    registers the id with the key's SHA-256 (POST /api/install-progress).
//!    Create only: an id that exists is refused.
//! 2. The id and the key go to the stack as launch parameters (StatusId,
//!    StatusKey, NoEcho). The stack's install helper reports each phase with
//!    the key (PUT /api/install-progress/:id).
//! 3. The guide reads it back with the key (POST /api/install-progress/:id/read).
//!
//! Without the key nobody can read or write a progress document, and the id
//! alone says nothing. A document holds the phase, a short message with
//! account numbers, ARNs and emails taken out, the install's address and the
//! DNS records for its domain: exactly what the guide shows.
//!
//! Stored on the settings table (pk INSTALL_PROGRESS, sk ID#<id>), expiring
//! 14 days after the last change. Hosted only: 404 on a self-hosted install.

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tracing::warn;

use crate::AppState;

const KEEP_DAYS: i64 = 14;
const PHASES: &[&str] = &[
    "starting",
    "preparing",
    "deploying",
    "finishing",
    "done",
    "failed",
];
/// The largest document accepted, in bytes of JSON.
const MAX_DOC: usize = 8 * 1024;

fn id_ok(id: &str) -> bool {
    id.len() == 16
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

fn key_ok(key: &str) -> bool {
    (32..=64).contains(&key.len()) && key.bytes().all(|b| b.is_ascii_alphanumeric())
}

fn hash_ok(h: &str) -> bool {
    h.len() == 64
        && h.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// The key's SHA-256, lowercase hex: what is stored and compared.
pub fn key_hash(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}

fn short(v: &Value, n: usize) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => {
            s.len() <= n
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
        }
        _ => false,
    }
}

fn host_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 253
        && s.split('.').count() >= 2
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_')
}

/// Why a progress document is refused, or None when it is fine. Only the
/// fields the guide shows, each with a plain shape.
pub fn problem(doc: &Value) -> Option<&'static str> {
    let Some(o) = doc.as_object() else {
        return Some("not an object");
    };
    const FIELDS: &[&str] = &[
        "schema",
        "phase",
        "message",
        "url",
        "version",
        "stage",
        "region",
        "updatedAt",
        "domain",
        "part",
        "step",
        "steps",
    ];
    if o.keys().any(|k| !FIELDS.contains(&k.as_str())) {
        return Some("unknown field");
    }
    if o.get("schema") != Some(&json!(1)) {
        return Some("schema");
    }
    if !o
        .get("phase")
        .and_then(Value::as_str)
        .is_some_and(|p| PHASES.contains(&p))
    {
        return Some("phase");
    }
    if !o
        .get("message")
        .is_none_or(|m| m.as_str().is_some_and(|s| s.len() <= 300) || m.is_null())
    {
        return Some("message");
    }
    if !o.get("url").is_none_or(|u| {
        u.is_null()
            || u.as_str().is_some_and(|s| {
                s.strip_prefix("https://")
                    .is_some_and(|h| host_ok(h.trim_end_matches('/')))
            })
    }) {
        return Some("url");
    }
    for k in ["version", "stage", "region"] {
        if !o.get(k).is_none_or(|v| short(v, 40)) {
            return Some("version, stage or region");
        }
    }
    if !o
        .get("updatedAt")
        .is_none_or(|v| v.as_str().is_some_and(|s| s.len() <= 40))
    {
        return Some("updatedAt");
    }
    let count =
        |v: Option<&Value>| v.is_none_or(|n| n.is_null() || n.as_u64().is_some_and(|n| n <= 1000));
    if !count(o.get("step")) || !count(o.get("steps")) {
        return Some("step");
    }
    if let Some(p) = o.get("part").filter(|p| !p.is_null()) {
        let name_ok = p.get("name").and_then(Value::as_str).is_some_and(|n| {
            (1..=40).contains(&n.len())
                && n.chars()
                    .all(|c| c.is_ascii_alphanumeric() || " .-".contains(c))
        });
        let extra = p.as_object().is_some_and(|m| {
            m.keys()
                .any(|k| !["name", "done", "total"].contains(&k.as_str()))
        });
        if !name_ok || extra || !count(p.get("done")) || !count(p.get("total")) {
            return Some("part");
        }
    }
    if let Some(d) = o.get("domain").filter(|d| !d.is_null()) {
        let Some(d) = d.as_object() else {
            return Some("domain");
        };
        if !d
            .get("hostname")
            .and_then(Value::as_str)
            .is_some_and(host_ok)
        {
            return Some("domain.hostname");
        }
        if !d.get("state").is_none_or(|s| short(s, 20)) {
            return Some("domain.state");
        }
        let records = d
            .get("records")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if records.len() > 4 {
            return Some("domain.records");
        }
        for r in records {
            let field = |k: &str, n: usize| {
                r.get(k)
                    .and_then(Value::as_str)
                    .is_some_and(|s| s.len() <= n)
            };
            let name_ok = r.get("name").and_then(Value::as_str).is_some_and(host_ok);
            if !(name_ok && field("value", 253) && field("type", 30) && field("purpose", 20)) {
                return Some("domain.records");
            }
        }
    }
    None
}

fn not_here() -> Response {
    StatusCode::NOT_FOUND.into_response()
}

fn refused(why: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": why }))).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Create {
    pub id: String,
    pub key_hash: String,
}

/// POST /api/install-progress (public): the guide registers a progress id
/// with its key's hash before it opens AWS. 201, or 409 when the id exists.
pub async fn create(State(state): State<Arc<AppState>>, Json(c): Json<Create>) -> Response {
    if common::public_urls::self_hosted() {
        return not_here();
    }
    if !id_ok(&c.id) || !hash_ok(&c.key_hash) {
        return refused("id is 16 lowercase letters and digits; key_hash is a SHA-256 in hex");
    }
    let now = chrono::Utc::now();
    let s = |v: &str| AttributeValue::S(v.to_string());
    let result = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s("INSTALL_PROGRESS"))
        .item("sk", s(&format!("ID#{}", c.id)))
        .item("key_hash", s(&c.key_hash))
        .item("created_at", s(&now.to_rfc3339()))
        .item(
            "ttl",
            AttributeValue::N((now.timestamp() + KEEP_DAYS * 86_400).to_string()),
        )
        .condition_expression("attribute_not_exists(sk)")
        .send()
        .await;
    match result {
        Ok(_) => StatusCode::CREATED.into_response(),
        Err(e)
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
        {
            (
                StatusCode::CONFLICT,
                Json(json!({ "error": "This progress id is taken. Start again for a new one." })),
            )
                .into_response()
        }
        Err(e) => {
            warn!(error = %e, "install progress: not created");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

/// The stored item when `key` matches its hash; Err is the answer to give.
async fn authorized(
    state: &AppState,
    id: &str,
    key: &str,
) -> Result<std::collections::HashMap<String, AttributeValue>, Response> {
    if !id_ok(id) || !key_ok(key) {
        return Err(refused("id or key"));
    }
    let got = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S("INSTALL_PROGRESS".into()))
        .key("sk", AttributeValue::S(format!("ID#{id}")))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| {
            warn!(error = %e, "install progress: not read");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        })?;
    let item = got.item.ok_or_else(not_here)?;
    let stored = item
        .get("key_hash")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    // Unknown id and wrong key answer the same.
    if !bool::from(key_hash(key).as_bytes().ct_eq(stored.as_bytes())) {
        return Err(not_here());
    }
    Ok(item)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub key: String,
    pub doc: Value,
}

/// PUT /api/install-progress/:id (public, the key in the body): the install
/// helper's report for one phase. 204.
pub async fn report(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(r): Json<Report>,
) -> Response {
    if common::public_urls::self_hosted() {
        return not_here();
    }
    let body = r.doc.to_string();
    if body.len() > MAX_DOC {
        return refused("the document is too large");
    }
    if let Some(why) = problem(&r.doc) {
        return refused(why);
    }
    if let Err(res) = authorized(&state, &id, &r.key).await {
        return res;
    }
    let now = chrono::Utc::now();
    let result = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S("INSTALL_PROGRESS".into()))
        .key("sk", AttributeValue::S(format!("ID#{id}")))
        .update_expression("SET doc = :d, updated_at = :t, #ttl = :ttl")
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":d", AttributeValue::S(body))
        .expression_attribute_values(":t", AttributeValue::S(now.to_rfc3339()))
        .expression_attribute_values(
            ":ttl",
            AttributeValue::N((now.timestamp() + KEEP_DAYS * 86_400).to_string()),
        )
        .send()
        .await;
    match result {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            warn!(error = %e, "install progress: not stored");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Read {
    pub key: String,
}

/// POST /api/install-progress/:id/read (public, the key in the body): the
/// latest report, or `{"doc": null}` while the install hasn't sent one (an
/// older release never does: the guide then reads its old progress file).
pub async fn read(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(r): Json<Read>,
) -> Response {
    if common::public_urls::self_hosted() {
        return not_here();
    }
    let item = match authorized(&state, &id, &r.key).await {
        Ok(i) => i,
        Err(res) => return res,
    };
    let doc = item
        .get("doc")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or(Value::Null);
    Json(json!({ "doc": doc })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good() -> Value {
        json!({
            "schema": 1, "phase": "done", "message": "CoderHelm is ready", "url": "https://d111test.cloudfront.net",
            "version": "v1.2.3", "stage": "prod", "region": "eu-west-1", "updatedAt": "2026-11-03T10:00:00Z",
            "domain": { "hostname": "coderhelm.example.com", "state": "validating", "records": [
                { "purpose": "certificate", "name": "_a1.coderhelm.example.com", "type": "CNAME", "value": "_b2.acm-validations.aws" },
                { "purpose": "address", "name": "coderhelm.example.com", "type": "CNAME", "value": "d111test.cloudfront.net" }
            ] }
        })
    }

    #[test]
    fn ids_keys_and_hashes_have_one_shape() {
        assert!(id_ok("abcd1234efgh5678"));
        assert!(!id_ok("ABCD1234efgh5678") && !id_ok("abcd1234") && !id_ok("abcd1234efgh567/"));
        assert!(key_ok(&"a".repeat(32)) && key_ok(&"Z9".repeat(32)));
        assert!(
            !key_ok(&"a".repeat(31))
                && !key_ok(&"a".repeat(65))
                && !key_ok(&format!("{}-", "a".repeat(32)))
        );
        let h = key_hash("k".repeat(40).as_str());
        assert!(hash_ok(&h));
        assert!(!hash_ok(&h.to_uppercase()) && !hash_ok(&h[1..]));
    }

    #[test]
    fn a_document_carries_only_what_the_guide_shows() {
        assert_eq!(problem(&good()), None);
        let with = |k: &str, v: Value| {
            let mut d = good();
            d[k] = v;
            problem(&d)
        };
        assert_eq!(with("schema", json!(2)), Some("schema"));
        assert_eq!(with("phase", json!("pwned")), Some("phase"));
        assert_eq!(with("message", json!("x".repeat(301))), Some("message"));
        assert_eq!(
            with("url", json!("http://d111test.cloudfront.net")),
            Some("url")
        );
        assert_eq!(with("url", json!("javascript:alert(1)")), Some("url"));
        assert_eq!(
            with("region", json!("eu-west-1<script>")),
            Some("version, stage or region")
        );
        assert_eq!(with("secret", json!("x")), Some("unknown field"));
        let mut d = good();
        d["domain"]["records"][0]["name"] = json!("not a host");
        assert_eq!(problem(&d), Some("domain.records"));
        let mut d = good();
        d["domain"] = Value::Null;
        d["url"] = Value::Null;
        assert_eq!(problem(&d), None);
        assert_eq!(problem(&json!([1])), Some("not an object"));
        assert_eq!(
            with("part", json!({ "name": "api", "done": 3, "total": 9 })),
            None
        );
        assert_eq!(
            with("part", json!({ "name": "<b>", "done": 3 })),
            Some("part")
        );
        assert_eq!(with("step", json!(-1)), Some("step"));
    }
}
