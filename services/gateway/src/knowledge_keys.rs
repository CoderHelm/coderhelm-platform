//! Knowledge keys: machine credentials that read a team's knowledge over
//! `/mcp`, for Amazon Bedrock AgentCore Gateway and other programs that sign
//! in without a person.
//!
//! An admin who manages knowledge bases makes a key in Settings and picks the
//! bases it reads. The secret is shown once; Coderhelm keeps only its SHA-256
//! hash. A key holds one scope, `knowledge:read`, and reaches only the
//! knowledge tools. It is used either way AgentCore Gateway's outbound
//! authorization supports for an MCP server target:
//!
//! - as an **API key**: `Authorization: Bearer chkb_<id>_<secret>` (or the
//!   `X-Api-Key` header) on every call;
//! - as **OAuth client credentials**: `client_id` `chkbc_<id>` and the key as
//!   `client_secret` at `POST /oauth/token` (`grant_type=client_credentials`),
//!   for an access token of an hour.
//!
//! Every call reads the key's record, so a revoked key stops at once, access
//! tokens included. A key acts within its maker's access, read on every call:
//! the bases it reads are the ones it was given that its maker can still read
//! in Chat (`routes::knowledge::chat_readable`), never a base read with each
//! person's own sign-in, and it stops when its maker leaves the team. Making,
//! revoking and every call are written to the team's audit log.
//!
//! Storage, in the settings table:
//! - `pk = KB_KEY#<id>`, `sk = KB_KEY`: the record a call is checked against;
//! - `pk = <team_id>`, `sk = KB_KEY#<id>`: the team's copy, for the list.

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem, Update};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Reading the team's knowledge bases.
pub use crate::mcp_oauth::KNOWLEDGE_SCOPE;

pub const TOKEN_PREFIX: &str = "chkb_";
pub const CLIENT_PREFIX: &str = "chkbc_";
/// The audience of a key's access token: accepted only by `/mcp`.
pub const ACCESS_AUDIENCE: &str = "coderhelm-mcp-key";
pub const ACCESS_TTL_SECS: u64 = 3600;
/// Live keys one team may have.
pub const MAX_KEYS_PER_TEAM: usize = 25;
pub const MAX_NAME: usize = 60;
pub const MAX_EXPIRY_DAYS: i64 = 365;

pub const KEY_PK_PREFIX: &str = "KB_KEY#";
pub const KEY_SK: &str = "KB_KEY";
pub const TEAM_SK_PREFIX: &str = "KB_KEY#";

// ── Secrets ─────────────────────────────────────────────────────────

pub struct NewKey {
    pub id: String,
    pub token: String,
    pub hash: String,
}

/// A new key: a ULID id and 32 random bytes, `chkb_<id>_<secret>`.
pub fn generate() -> NewKey {
    use base64::Engine;
    use rand::RngCore;
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let mut secret = [0u8; 32];
    rand::rng().fill_bytes(&mut secret);
    let secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    let token = format!("{TOKEN_PREFIX}{id}_{secret}");
    let hash = hash(&token);
    NewKey { id, token, hash }
}

pub fn hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn hash_matches(token: &str, stored: &str) -> bool {
    let got = hash(token);
    !stored.is_empty()
        && got.len() == stored.len()
        && bool::from(got.as_bytes().ct_eq(stored.as_bytes()))
}

/// The key id inside a well-formed key.
pub fn token_id(token: &str) -> Option<&str> {
    let rest = token.strip_prefix(TOKEN_PREFIX)?;
    let (id, secret) = rest.split_once('_')?;
    let id_ok = id.len() == 26 && id.chars().all(|c| c.is_ascii_alphanumeric());
    let secret_ok = secret.len() == 43
        && secret
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    (id_ok && secret_ok).then_some(id)
}

/// A key's OAuth client id.
pub fn client_id(key_id: &str) -> String {
    format!("{CLIENT_PREFIX}{key_id}")
}

/// What the page shows of a key after it is made.
pub fn hint(token: &str) -> String {
    let start: String = token.chars().take(TOKEN_PREFIX.len() + 6).collect();
    let end: String = token
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{start}...{end}")
}

// ── Stored keys ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredKey {
    pub id: String,
    pub team_id: String,
    pub name: String,
    pub hash: String,
    /// The bases it reads, by id.
    pub bases: Vec<String>,
    /// The maker's user id and email: the key acts within their access.
    pub created_by: String,
    pub created_by_email: String,
    pub created_at: String,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub revoked_at: Option<String>,
    #[serde(default)]
    pub revoked_by: Option<String>,
    #[serde(default)]
    pub last_used_at: Option<String>,
    pub hint: String,
}

impl StoredKey {
    pub fn is_expired(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        self.expires_at
            .as_deref()
            .and_then(|e| chrono::DateTime::parse_from_rfc3339(e).ok())
            .is_some_and(|e| e <= now)
    }

    pub fn is_live(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        self.revoked_at.is_none() && !self.is_expired(now)
    }
}

/// Why a key or its token was refused (each a `401`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    Invalid,
    Expired,
    Revoked,
}

impl Refusal {
    pub fn description(self) -> &'static str {
        match self {
            Refusal::Invalid => "The key isn't valid.",
            Refusal::Expired => "The key or its access token has expired.",
            Refusal::Revoked => "This key was revoked in Coderhelm.",
        }
    }
}

#[async_trait::async_trait]
pub trait KeyStore: Send + Sync {
    async fn get(&self, id: &str) -> Result<Option<StoredKey>, String>;
    async fn create(&self, k: &StoredKey) -> Result<(), String>;
    async fn list(&self, team_id: &str) -> Result<Vec<StoredKey>, String>;
    /// Marks the key revoked (both records). False when it isn't the team's.
    async fn revoke(&self, team_id: &str, id: &str, by: &str, at: &str) -> Result<bool, String>;
    async fn note_use(&self, k: &StoredKey, at: &str) -> Result<(), String>;
}

/// The live key behind `token` (a key itself).
pub async fn authenticate_secret(
    store: &dyn KeyStore,
    token: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Result<StoredKey, Refusal>, String> {
    let Some(id) = token_id(token) else {
        return Ok(Err(Refusal::Invalid));
    };
    let Some(k) = store.get(id).await? else {
        return Ok(Err(Refusal::Invalid));
    };
    if !hash_matches(token, &k.hash) {
        return Ok(Err(Refusal::Invalid));
    }
    Ok(live(k, now))
}

fn live(k: StoredKey, now: chrono::DateTime<chrono::Utc>) -> Result<StoredKey, Refusal> {
    if k.revoked_at.is_some() {
        Err(Refusal::Revoked)
    } else if k.is_expired(now) {
        Err(Refusal::Expired)
    } else {
        Ok(k)
    }
}

// ── Client credentials ──────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyClaims {
    pub kid: String,
    pub team_id: String,
    pub scope: String,
    pub aud: String,
    pub iss: String,
    pub iat: u64,
    pub exp: u64,
}

pub fn issue_access(
    k: &StoredKey,
    issuer: &str,
    secret: &str,
    now_secs: u64,
) -> Result<String, jsonwebtoken::errors::Error> {
    encode(
        &Header::default(),
        &KeyClaims {
            kid: k.id.clone(),
            team_id: k.team_id.clone(),
            scope: KNOWLEDGE_SCOPE.into(),
            aud: ACCESS_AUDIENCE.into(),
            iss: issuer.into(),
            iat: now_secs,
            exp: now_secs + ACCESS_TTL_SECS,
        },
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// The claims of a key's access token, checked against `now_secs`. `None`
/// when it isn't one (another kind of token may still be).
pub fn read_access(token: &str, secret: &str, now_secs: u64) -> Option<Result<KeyClaims, Refusal>> {
    let mut v = Validation::default();
    v.set_audience(&[ACCESS_AUDIENCE]);
    v.validate_exp = false;
    let c = decode::<KeyClaims>(token, &DecodingKey::from_secret(secret.as_bytes()), &v)
        .ok()?
        .claims;
    if c.exp <= now_secs {
        return Some(Err(Refusal::Expired));
    }
    Some(Ok(c))
}

/// The live key behind a key's access token: the record is read on every
/// call, so revoking ends its tokens too.
pub async fn authenticate_access(
    store: &dyn KeyStore,
    claims: &KeyClaims,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Result<StoredKey, Refusal>, String> {
    let Some(k) = store.get(&claims.kid).await? else {
        return Ok(Err(Refusal::Revoked));
    };
    if k.team_id != claims.team_id {
        return Ok(Err(Refusal::Invalid));
    }
    Ok(live(k, now))
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientCredentialsAnswer {
    pub access_token: String,
    pub token_type: &'static str,
    pub expires_in: u64,
    pub scope: String,
}

/// `grant_type=client_credentials`: the client id and secret from the form
/// (`client_secret_post`) or the Basic header (`client_secret_basic`). The
/// only scope is `knowledge:read`; asking for another is refused.
pub async fn client_credentials(
    store: &dyn KeyStore,
    client_id: Option<&str>,
    client_secret: Option<&str>,
    scope: Option<&str>,
    issuer: &str,
    secret: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Result<ClientCredentialsAnswer, (&'static str, &'static str)>, String> {
    let invalid = (
        "invalid_client",
        "The client id or secret is wrong, or the key was revoked.",
    );
    let (Some(cid), Some(token)) = (client_id, client_secret) else {
        return Ok(Err(invalid));
    };
    if let Some(s) = scope {
        if s.split_whitespace().any(|w| w != KNOWLEDGE_SCOPE) {
            return Ok(Err((
                "invalid_scope",
                "A knowledge key holds only knowledge:read.",
            )));
        }
    }
    let k = match authenticate_secret(store, token, now).await? {
        Ok(k) => k,
        Err(_) => return Ok(Err(invalid)),
    };
    if client_id_matches(cid, &k.id).is_none() {
        return Ok(Err(invalid));
    }
    let now_secs = now.timestamp().max(0) as u64;
    let access = issue_access(&k, issuer, secret, now_secs).map_err(|e| e.to_string())?;
    Ok(Ok(ClientCredentialsAnswer {
        access_token: access,
        token_type: "Bearer",
        expires_in: ACCESS_TTL_SECS,
        scope: KNOWLEDGE_SCOPE.into(),
    }))
}

fn client_id_matches(given: &str, id: &str) -> Option<()> {
    (given.strip_prefix(CLIENT_PREFIX)? == id).then_some(())
}

/// The client id and secret of an `Authorization: Basic` header (RFC 6749
/// 2.3.1: each form-urlencoded, then joined by `:`).
pub fn basic_credentials(headers: &axum::http::HeaderMap) -> Option<(String, String)> {
    use base64::Engine;
    let v = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, b64) = v.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let raw = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .ok()?;
    let raw = String::from_utf8(raw).ok()?;
    let (id, secret) = raw.split_once(':')?;
    let dec = |s: &str| {
        urlencoding::decode(&s.replace('+', " "))
            .map(|c| c.into_owned())
            .ok()
    };
    Some((dec(id)?, dec(secret)?))
}

// ── Making a key ────────────────────────────────────────────────────

/// The name and bases of a new key, checked: a name, at least one base,
/// each one `readable` (the bases its maker can give it).
pub fn check_new(
    name: &str,
    bases: &[String],
    readable: &[String],
    expires_in_days: Option<i64>,
    live_keys: usize,
) -> Result<(String, Vec<String>, Option<i64>), String> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > MAX_NAME {
        return Err(format!("Name the key (up to {MAX_NAME} characters)."));
    }
    if live_keys >= MAX_KEYS_PER_TEAM {
        return Err(format!(
            "A team can have {MAX_KEYS_PER_TEAM} knowledge keys. Revoke one you no longer use first."
        ));
    }
    let mut out: Vec<String> = Vec::new();
    for b in bases {
        if !readable.contains(b) {
            return Err("Pick knowledge bases you can read, and that aren't read with each person's own sign-in.".into());
        }
        if !out.contains(b) {
            out.push(b.clone());
        }
    }
    if out.is_empty() {
        return Err("Pick at least one knowledge base the key reads.".into());
    }
    let days = match expires_in_days {
        None | Some(0) => None,
        Some(d) if (1..=MAX_EXPIRY_DAYS).contains(&d) => Some(d),
        Some(_) => {
            return Err(format!(
                "A key lasts 1 to {MAX_EXPIRY_DAYS} days, or never expires."
            ))
        }
    };
    Ok((name.to_string(), out, days))
}

// ── DynamoDB ────────────────────────────────────────────────────────

pub struct DynamoKeys<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
}

pub fn store(state: &crate::AppState) -> DynamoKeys<'_> {
    DynamoKeys {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn err<E: std::error::Error + 'static>(e: aws_sdk_dynamodb::error::SdkError<E>) -> String {
    format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e))
}

fn key_of(item: &std::collections::HashMap<String, AttributeValue>) -> Option<StoredKey> {
    serde_json::from_str(item.get("data")?.as_s().ok()?).ok()
}

#[async_trait::async_trait]
impl KeyStore for DynamoKeys<'_> {
    async fn get(&self, id: &str) -> Result<Option<StoredKey>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(&format!("{KEY_PK_PREFIX}{id}")))
            .key("sk", s(KEY_SK))
            .consistent_read(true)
            .send()
            .await
            .map_err(err)?;
        Ok(r.item().and_then(key_of))
    }

    async fn create(&self, k: &StoredKey) -> Result<(), String> {
        let data = serde_json::to_string(k).map_err(|e| e.to_string())?;
        let put = |pk: String, sk: String| {
            Put::builder()
                .table_name(self.table)
                .item("pk", s(&pk))
                .item("sk", s(&sk))
                .item("data", s(&data))
                .condition_expression("attribute_not_exists(pk)")
                .build()
                .map(|p| TransactWriteItem::builder().put(p).build())
                .map_err(|e| e.to_string())
        };
        self.dynamo
            .transact_write_items()
            .transact_items(put(format!("{KEY_PK_PREFIX}{}", k.id), KEY_SK.into())?)
            .transact_items(put(k.team_id.clone(), format!("{TEAM_SK_PREFIX}{}", k.id))?)
            .send()
            .await
            .map(|_| ())
            .map_err(err)
    }

    async fn list(&self, team_id: &str) -> Result<Vec<StoredKey>, String> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let r = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                .expression_attribute_values(":pk", s(team_id))
                .expression_attribute_values(":p", s(TEAM_SK_PREFIX))
                .set_exclusive_start_key(start.take())
                .send()
                .await
                .map_err(err)?;
            out.extend(r.items().iter().filter_map(key_of));
            match r.last_evaluated_key() {
                Some(k) if !k.is_empty() => start = Some(k.clone()),
                _ => break,
            }
        }
        out.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(out)
    }

    async fn revoke(&self, team_id: &str, id: &str, by: &str, at: &str) -> Result<bool, String> {
        let Some(mut k) = self.get(id).await? else {
            return Ok(false);
        };
        if k.team_id != team_id {
            return Ok(false);
        }
        if k.revoked_at.is_some() {
            return Ok(true);
        }
        k.revoked_at = Some(at.to_string());
        k.revoked_by = Some(by.to_string());
        let data = serde_json::to_string(&k).map_err(|e| e.to_string())?;
        let upd = |pk: String, sk: String| {
            Update::builder()
                .table_name(self.table)
                .key("pk", s(&pk))
                .key("sk", s(&sk))
                .update_expression("SET #d = :d")
                .expression_attribute_names("#d", "data")
                .expression_attribute_values(":d", s(&data))
                .condition_expression("attribute_exists(pk)")
                .build()
                .map(|u| TransactWriteItem::builder().update(u).build())
                .map_err(|e| e.to_string())
        };
        self.dynamo
            .transact_write_items()
            .transact_items(upd(format!("{KEY_PK_PREFIX}{id}"), KEY_SK.into())?)
            .transact_items(upd(team_id.to_string(), format!("{TEAM_SK_PREFIX}{id}"))?)
            .send()
            .await
            .map(|_| true)
            .map_err(err)
    }

    async fn note_use(&self, k: &StoredKey, at: &str) -> Result<(), String> {
        // The team's copy only: the record a call is checked against keeps
        // its data as made and revoked.
        let mut copy = k.clone();
        copy.last_used_at = Some(at.to_string());
        let data = serde_json::to_string(&copy).map_err(|e| e.to_string())?;
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", s(&k.team_id))
            .key("sk", s(&format!("{TEAM_SK_PREFIX}{}", k.id)))
            .update_expression("SET #d = :d")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_names("#d", "data")
            .expression_attribute_values(":d", s(&data))
            .send()
            .await
            .map(|_| ())
            .map_err(err)
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct MemKeys(pub Mutex<HashMap<String, StoredKey>>);

    #[async_trait::async_trait]
    impl KeyStore for MemKeys {
        async fn get(&self, id: &str) -> Result<Option<StoredKey>, String> {
            Ok(self.0.lock().unwrap().get(id).cloned())
        }
        async fn create(&self, k: &StoredKey) -> Result<(), String> {
            self.0.lock().unwrap().insert(k.id.clone(), k.clone());
            Ok(())
        }
        async fn list(&self, team_id: &str) -> Result<Vec<StoredKey>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .values()
                .filter(|k| k.team_id == team_id)
                .cloned()
                .collect())
        }
        async fn revoke(
            &self,
            team_id: &str,
            id: &str,
            by: &str,
            at: &str,
        ) -> Result<bool, String> {
            let mut m = self.0.lock().unwrap();
            match m.get_mut(id) {
                Some(k) if k.team_id == team_id => {
                    k.revoked_at = Some(at.into());
                    k.revoked_by = Some(by.into());
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        async fn note_use(&self, _: &StoredKey, _: &str) -> Result<(), String> {
            Ok(())
        }
    }

    pub fn make(store: &MemKeys, team: &str, bases: &[&str]) -> (StoredKey, String) {
        let n = generate();
        let k = StoredKey {
            id: n.id.clone(),
            team_id: team.into(),
            name: "AgentCore".into(),
            hash: n.hash.clone(),
            bases: bases.iter().map(|b| b.to_string()).collect(),
            created_by: "USER#a".into(),
            created_by_email: "a@x.com".into(),
            created_at: "2026-11-10T00:00:00Z".into(),
            expires_at: None,
            revoked_at: None,
            revoked_by: None,
            last_used_at: None,
            hint: hint(&n.token),
        };
        store.0.lock().unwrap().insert(k.id.clone(), k.clone());
        (k, n.token)
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;

    const SECRET: &str = "test-jwt-secret-at-least-32-chars!";

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-11-10T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[test]
    fn keys_are_well_formed_and_only_their_hash_is_kept() {
        let n = generate();
        assert!(n.token.starts_with("chkb_"));
        assert_eq!(token_id(&n.token), Some(n.id.as_str()));
        assert_eq!(n.hash, hash(&n.token));
        assert!(!n.hash.contains(&n.token));
        assert!(hint(&n.token).contains("..."));
        assert_eq!(token_id("chk_nope"), None);
        // A team API key is not a knowledge key.
        assert_eq!(token_id(&crate::api_keys::generate().token), None);
    }

    #[tokio::test]
    async fn a_key_works_until_it_is_revoked_and_its_tokens_stop_with_it() {
        let store = MemKeys::default();
        let (k, token) = make(&store, "TEAM#a", &["kb1"]);
        assert_eq!(
            authenticate_secret(&store, &token, now())
                .await
                .unwrap()
                .unwrap()
                .id,
            k.id
        );
        // A wrong secret for the same id.
        let forged = format!("{}{}_{}", TOKEN_PREFIX, k.id, "A".repeat(43));
        assert_eq!(
            authenticate_secret(&store, &forged, now()).await.unwrap(),
            Err(Refusal::Invalid)
        );
        // Client credentials, both ways of sending them.
        let cid = client_id(&k.id);
        let t = client_credentials(
            &store,
            Some(&cid),
            Some(&token),
            Some("knowledge:read"),
            "https://x",
            SECRET,
            now(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(t.scope, "knowledge:read");
        assert_eq!(t.token_type, "Bearer");
        let claims = read_access(&t.access_token, SECRET, now().timestamp() as u64)
            .unwrap()
            .unwrap();
        assert_eq!(
            authenticate_access(&store, &claims, now())
                .await
                .unwrap()
                .unwrap()
                .id,
            k.id
        );
        // Another key's client id, another scope or no secret: refused.
        let (other, _) = make(&store, "TEAM#a", &["kb1"]);
        assert!(client_credentials(
            &store,
            Some(&client_id(&other.id)),
            Some(&token),
            None,
            "https://x",
            SECRET,
            now()
        )
        .await
        .unwrap()
        .is_err());
        assert_eq!(
            client_credentials(
                &store,
                Some(&cid),
                Some(&token),
                Some("runs:write"),
                "https://x",
                SECRET,
                now()
            )
            .await
            .unwrap()
            .unwrap_err()
            .0,
            "invalid_scope"
        );
        assert!(
            client_credentials(&store, Some(&cid), None, None, "https://x", SECRET, now())
                .await
                .unwrap()
                .is_err()
        );
        // Revoked: the key and the token it already got both stop.
        assert!(store
            .revoke("TEAM#a", &k.id, "USER#b", "2026-11-10T12:01:00Z")
            .await
            .unwrap());
        assert_eq!(
            authenticate_secret(&store, &token, now()).await.unwrap(),
            Err(Refusal::Revoked)
        );
        assert_eq!(
            authenticate_access(&store, &claims, now()).await.unwrap(),
            Err(Refusal::Revoked)
        );
        assert!(client_credentials(
            &store,
            Some(&cid),
            Some(&token),
            None,
            "https://x",
            SECRET,
            now()
        )
        .await
        .unwrap()
        .is_err());
        // Another team can't revoke it.
        assert!(!store
            .revoke("TEAM#b", &other.id, "USER#x", "t")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn an_access_token_expires_and_is_never_another_kind() {
        let store = MemKeys::default();
        let (k, _) = make(&store, "TEAM#a", &["kb1"]);
        let t = issue_access(&k, "https://x", SECRET, 1_000).unwrap();
        assert_eq!(
            read_access(&t, SECRET, 1_000 + ACCESS_TTL_SECS),
            Some(Err(Refusal::Expired))
        );
        // A person's MCP access token, or a session, is not a key's token.
        let session =
            crate::auth::jwt::create_token("u", "t", "e@x.com", "owner", None, SECRET, 300)
                .unwrap();
        assert_eq!(read_access(&session, SECRET, 1_000), None);
        // A token claiming another team's key is refused.
        let mut claims = read_access(&t, SECRET, 1_000).unwrap().unwrap();
        claims.team_id = "TEAM#b".into();
        assert_eq!(
            authenticate_access(&store, &claims, now()).await.unwrap(),
            Err(Refusal::Invalid)
        );
    }

    #[test]
    fn basic_credentials_are_read_as_rfc_6749_says() {
        use base64::Engine;
        let mut h = axum::http::HeaderMap::new();
        let v = base64::engine::general_purpose::STANDARD.encode("chkbc_abc:chkb_x%2By");
        h.insert(
            axum::http::header::AUTHORIZATION,
            format!("Basic {v}").parse().unwrap(),
        );
        assert_eq!(
            basic_credentials(&h),
            Some(("chkbc_abc".into(), "chkb_x+y".into()))
        );
    }

    #[test]
    fn a_new_key_reads_only_bases_its_maker_can_give() {
        let readable = vec!["kb1".to_string(), "kb2".to_string()];
        let (n, b, d) = check_new(
            " Bedrock ",
            &["kb1".into(), "kb1".into()],
            &readable,
            Some(90),
            0,
        )
        .unwrap();
        assert_eq!(
            (n.as_str(), b, d),
            ("Bedrock", vec!["kb1".to_string()], Some(90))
        );
        assert!(check_new("x", &["kb3".into()], &readable, None, 0).is_err());
        assert!(check_new("x", &[], &readable, None, 0).is_err());
        assert!(check_new("", &["kb1".into()], &readable, None, 0).is_err());
        assert!(check_new("x", &["kb1".into()], &readable, Some(400), 0).is_err());
        assert!(check_new("x", &["kb1".into()], &readable, None, MAX_KEYS_PER_TEAM).is_err());
    }
}
