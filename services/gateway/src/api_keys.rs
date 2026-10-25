//! Team API keys: how a program outside the dashboard calls the public API.
//!
//! An admin makes a key on the dashboard's API keys page. The key is shown
//! once; CoderHelm keeps only its SHA-256 hash. A request sends it as
//! `Authorization: Bearer chk_<id>_<secret>`.
//!
//! What a key may do is decided in the same place as everything else
//! (`permissions::resolve_access`), from a [`Holder`] built for the key:
//!
//! - its scopes are capability keys from `permissions::CAPABILITIES`, limited
//!   to the capabilities that cover at least one public route
//!   ([`key_scopes`]); `view` is always included;
//! - on every request the scopes are cut down to what the admin who made the
//!   key holds at that moment, so a key never does more than its maker can,
//!   and stops working when its maker leaves the team;
//! - it reaches only the routes marked public in `openapi::PUBLIC_ROUTES`.
//!
//! Storage, in the settings table:
//! - `pk = API_KEY#<id>`, `sk = API_KEY`: the record a request is checked
//!   against (one read, by the id inside the token);
//! - `pk = <team_id>`, `sk = API_KEY#<id>`: the team's copy, listed on the
//!   API keys page, with `last_used_at`;
//! - `pk = API_KEY#<id>`, `sk = RATE#<minute>`: the per-key request counter
//!   for one minute, removed by the table's TTL;
//! - `pk = <team_id>`, `sk = ROLE_AUDIT#<day>T23:59:59.999Z#KEY_USE#<id>`:
//!   the day's use summary in the team's audit log (created and revoked keys
//!   are logged there too, by `routes::api_keys`).

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem, Update};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use subtle::ConstantTimeEq;

use crate::permissions::{self, Capability, CustomRoleRef, Holder, CAPABILITIES};

/// Every key starts with this, so secret scanners and people can spot one.
pub const TOKEN_PREFIX: &str = "chk_";
/// Requests per minute a key gets unless its maker picks another number.
pub const DEFAULT_RATE_PER_MINUTE: u32 = 600;
pub const MIN_RATE_PER_MINUTE: u32 = 10;
pub const MAX_RATE_PER_MINUTE: u32 = 3000;
/// Live keys a team may have at once.
pub const MAX_KEYS_PER_TEAM: usize = 50;
pub const MAX_NAME: usize = 60;
/// The longest a key may be set to last.
pub const MAX_EXPIRY_DAYS: i64 = 365;
/// The length of a rate window, in seconds.
pub const RATE_WINDOW_SECS: u64 = 60;

pub const KEY_PK_PREFIX: &str = "API_KEY#";
pub const KEY_SK: &str = "API_KEY";
pub const TEAM_SK_PREFIX: &str = "API_KEY#";

// ── Tokens ──────────────────────────────────────────────────────────

/// A freshly made key: the token is shown once, the hash is kept.
pub struct NewToken {
    pub id: String,
    pub token: String,
    pub hash: String,
}

/// A new key: a ULID id and 32 random bytes, `chk_<id>_<secret>`.
pub fn generate() -> NewToken {
    use base64::Engine;
    use rand::RngCore;
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let mut secret = [0u8; 32];
    rand::rng().fill_bytes(&mut secret);
    let secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    let token = format!("{TOKEN_PREFIX}{id}_{secret}");
    let hash = hash_token(&token);
    NewToken { id, token, hash }
}

/// The SHA-256 of the whole token, as hex. The secret carries 256 bits of
/// randomness, so a fast hash is the right one (nothing to guess).
pub fn hash_token(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// The key id inside a well-formed token, or `None`.
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

/// What the dashboard shows of a key after it is made: the start and the
/// last four characters.
pub fn token_hint(token: &str) -> String {
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

/// Does `token` match the kept hash? Constant time.
pub fn hash_matches(token: &str, stored_hash: &str) -> bool {
    let got = hash_token(token);
    got.len() == stored_hash.len() && bool::from(got.as_bytes().ct_eq(stored_hash.as_bytes()))
}

/// The bearer token of a request, when it is an API key.
pub fn bearer_key(headers: &axum::http::HeaderMap) -> Option<&str> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    token.starts_with(TOKEN_PREFIX).then_some(token)
}

// ── Scopes ──────────────────────────────────────────────────────────

/// Always held by a key: reading what the team can see.
pub const BASE_SCOPE: &str = "view";

/// The capabilities a key can be given: those that cover at least one public
/// route, in table order. Derived from the public route list, so a route
/// made public later brings its capability with it.
pub fn key_scopes() -> Vec<&'static Capability> {
    let public: BTreeSet<&str> = crate::openapi::PUBLIC_ROUTES
        .iter()
        .filter_map(|r| r.key_handler())
        .collect();
    CAPABILITIES
        .iter()
        .filter(|c| c.key == BASE_SCOPE || c.handlers.iter().any(|h| public.contains(h)))
        .collect()
}

fn is_key_scope(key: &str) -> bool {
    key_scopes().iter().any(|c| c.key == key)
}

/// Why a set of scopes can't go on a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeError {
    Unknown(String),
    /// A capability that no public route needs.
    NotForKeys(&'static Capability),
    /// The admin making the key doesn't hold it.
    MoreThanYouHave(&'static Capability),
}

impl ScopeError {
    pub fn message(&self) -> String {
        match self {
            Self::Unknown(k) => format!("\"{k}\" isn't a scope."),
            Self::NotForKeys(c) => format!(
                "\"{}\" can't go on an API key: no public endpoint needs it.",
                c.key
            ),
            Self::MoreThanYouHave(c) => format!(
                "You can't give a key \"{}\", because your own role doesn't include it.",
                c.key
            ),
        }
    }
}

/// The scopes a new key will hold, checked: each is a key scope and held by
/// the admin making it. `view` is added.
pub fn checked_scopes(
    actor: &Holder,
    requested: &[String],
) -> Result<BTreeSet<&'static str>, ScopeError> {
    let mut out = BTreeSet::from([BASE_SCOPE]);
    for key in requested {
        let cap = permissions::capability(key).ok_or_else(|| ScopeError::Unknown(key.clone()))?;
        if !is_key_scope(cap.key) {
            return Err(ScopeError::NotForKeys(cap));
        }
        if !actor.holds(cap.key) {
            return Err(ScopeError::MoreThanYouHave(cap));
        }
        out.insert(cap.key);
    }
    Ok(out)
}

/// What a key holds on this request: its stored scopes that are still key
/// scopes, cut down to what its maker holds now. Checked by
/// `permissions::resolve_access` like any role.
pub fn key_holder(key: &StoredKey, maker: &Holder) -> Holder {
    let caps: BTreeSet<&'static str> = key
        .scopes
        .iter()
        .chain(std::iter::once(&BASE_SCOPE.to_string()))
        .filter_map(|k| permissions::capability(k))
        .filter(|c| is_key_scope(c.key) && maker.holds(c.key))
        .map(|c| c.key)
        .collect();
    Holder {
        role: format!("apikey:{}", key.id),
        caps,
        custom: Some(CustomRoleRef {
            id: key.id.clone(),
            name: key.name.clone(),
        }),
    }
}

// ── Stored keys ─────────────────────────────────────────────────────

/// One key as kept. The secret is never kept, only its hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredKey {
    pub id: String,
    pub team_id: String,
    pub name: String,
    pub hash: String,
    /// Capability keys.
    pub scopes: Vec<String>,
    /// The maker's user id; the key acts within their access.
    pub created_by: String,
    pub created_by_name: String,
    pub created_at: String,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub revoked_at: Option<String>,
    #[serde(default)]
    pub revoked_by: Option<String>,
    #[serde(default)]
    pub last_used_at: Option<String>,
    pub rate_limit_per_minute: u32,
    /// The start and end of the token, to tell keys apart.
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

/// Why a key was not accepted. Each is a `401` with this code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRefusal {
    Invalid,
    Revoked,
    Expired,
    /// The admin who made it is no longer on the team.
    MakerGone,
}

impl KeyRefusal {
    pub fn code(self) -> &'static str {
        match self {
            Self::Invalid => "invalid_api_key",
            Self::Revoked => "api_key_revoked",
            Self::Expired => "api_key_expired",
            Self::MakerGone => "api_key_owner_left",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::Invalid => "This API key isn't valid. Check that it was copied whole.",
            Self::Revoked => "This API key was revoked. Ask an admin for a new one.",
            Self::Expired => "This API key has expired. Ask an admin for a new one.",
            Self::MakerGone => {
                "The person who made this API key is no longer on the team, so it no longer works."
            }
        }
    }
}

/// Checks a token against its kept record. A missing record, a wrong hash
/// and a malformed token all read as `Invalid`, so nothing is learned about
/// which keys exist.
pub fn check_token<'a>(
    token: &str,
    stored: Option<&'a StoredKey>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<&'a StoredKey, KeyRefusal> {
    let id = token_id(token).ok_or(KeyRefusal::Invalid)?;
    let key = stored.filter(|k| k.id == id).ok_or(KeyRefusal::Invalid)?;
    if !hash_matches(token, &key.hash) {
        return Err(KeyRefusal::Invalid);
    }
    if key.revoked_at.is_some() {
        return Err(KeyRefusal::Revoked);
    }
    if key.is_expired(now) {
        return Err(KeyRefusal::Expired);
    }
    Ok(key)
}

/// The maker of a key, as their member record has them now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Maker {
    pub role: String,
    pub email: String,
    pub github_login: Option<String>,
}

// ── Rate limit ──────────────────────────────────────────────────────

/// One request's place in its key's minute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateWindow {
    pub allowed: bool,
    pub limit: u32,
    pub remaining: u32,
    /// Seconds until the window starts over.
    pub reset_secs: u64,
}

/// The window a timestamp falls in.
pub fn rate_window(now_secs: u64) -> u64 {
    now_secs / RATE_WINDOW_SECS
}

/// `count` is this request's number in the key's current minute (1 for the
/// first).
pub fn rate_decision(count: u64, limit: u32, now_secs: u64) -> RateWindow {
    let reset_secs = RATE_WINDOW_SECS - (now_secs % RATE_WINDOW_SECS);
    RateWindow {
        allowed: count <= u64::from(limit),
        limit,
        remaining: u64::from(limit).saturating_sub(count) as u32,
        reset_secs,
    }
}

// ── Where keys are kept ─────────────────────────────────────────────

/// A trait so the rules can be tested without DynamoDB.
pub trait KeyStore {
    /// The record a request is checked against.
    async fn get(&self, id: &str) -> Result<Option<StoredKey>, String>;
    /// The team's keys, live and revoked, newest first.
    async fn list(&self, team_id: &str) -> Result<Vec<StoredKey>, String>;
    /// Writes both records at once.
    async fn create(&self, key: &StoredKey) -> Result<(), String>;
    /// Marks a key of this team revoked. `false` when the team has no such
    /// live key.
    async fn revoke(&self, team_id: &str, id: &str, by: &str, at: &str) -> Result<bool, String>;
    /// Counts one request in the key's window and returns its number.
    async fn count_request(&self, id: &str, window: u64) -> Result<u64, String>;
    /// Notes a use: `last_used_at` and the day's summary in the audit log.
    /// Called once per key per minute.
    async fn note_use(
        &self,
        key: &StoredKey,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), String>;
    /// The key's maker, when they are still an active member of its team.
    async fn maker(&self, team_id: &str, user_id: &str) -> Result<Option<Maker>, String>;
}

/// A key that was accepted, with its maker.
#[derive(Debug, Clone)]
pub struct Accepted {
    pub key: StoredKey,
    pub maker: Maker,
    pub rate: RateWindow,
}

/// What became of a request carrying an API key.
#[derive(Debug, Clone)]
pub enum Outcome {
    Accepted(Box<Accepted>),
    Refused(KeyRefusal),
    RateLimited(RateWindow),
}

/// Checks a key and counts the request against its rate limit. The store
/// failing is an error (the caller answers 503), never a pass.
pub async fn authenticate(
    store: &impl KeyStore,
    token: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Outcome, String> {
    let Some(id) = token_id(token) else {
        return Ok(Outcome::Refused(KeyRefusal::Invalid));
    };
    let stored = store.get(id).await?;
    let key = match check_token(token, stored.as_ref(), now) {
        Ok(k) => k.clone(),
        Err(r) => return Ok(Outcome::Refused(r)),
    };
    let Some(maker) = store.maker(&key.team_id, &key.created_by).await? else {
        return Ok(Outcome::Refused(KeyRefusal::MakerGone));
    };
    let now_secs = now.timestamp().max(0) as u64;
    let count = store.count_request(&key.id, rate_window(now_secs)).await?;
    let rate = rate_decision(count, key.rate_limit_per_minute, now_secs);
    if !rate.allowed {
        return Ok(Outcome::RateLimited(rate));
    }
    if count == 1 {
        // The first request of this minute, on any instance: one write a
        // minute at most, and a failure never refuses the request.
        if let Err(e) = store.note_use(&key, now).await {
            tracing::warn!(error = %e, key_id = %key.id, "API key use not noted");
        }
    }
    Ok(Outcome::Accepted(Box::new(Accepted { key, maker, rate })))
}

/// The audit log's sort key for a key's use summary on one day.
pub fn use_summary_sk(key_id: &str, day: &str) -> String {
    format!(
        "{}{day}T23:59:59.999Z#KEY_USE#{key_id}",
        crate::routes::roles::AUDIT_PREFIX
    )
}

/// The sentence the audit log shows for a day's use.
pub fn use_summary_detail(minutes: u64, last_at: &str) -> String {
    let time = last_at.get(11..16).unwrap_or(last_at);
    let plural = if minutes == 1 { "" } else { "s" };
    format!("Used during {minutes} minute{plural} today, last at {time} UTC.")
}

// ── DynamoDB ────────────────────────────────────────────────────────

pub struct DynamoKeys<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub settings_table: &'a str,
    pub users_table: &'a str,
}

pub fn store(state: &crate::AppState) -> DynamoKeys<'_> {
    DynamoKeys {
        dynamo: &state.dynamo,
        settings_table: &state.config.settings_table_name,
        users_table: &state.config.users_table_name,
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn err<E: std::error::Error + 'static>(e: aws_sdk_dynamodb::error::SdkError<E>) -> String {
    format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e))
}

fn key_of(item: &HashMap<String, AttributeValue>) -> Option<StoredKey> {
    let mut key: StoredKey = item
        .get("data")
        .and_then(|v| v.as_s().ok())
        .and_then(|d| serde_json::from_str(d).ok())?;
    if let Some(at) = item.get("last_used_at").and_then(|v| v.as_s().ok()) {
        key.last_used_at = Some(at.clone());
    }
    if let Some(at) = item.get("revoked_at").and_then(|v| v.as_s().ok()) {
        key.revoked_at = Some(at.clone());
        key.revoked_by = item.get("revoked_by").and_then(|v| v.as_s().ok()).cloned();
    }
    Some(key)
}

impl KeyStore for DynamoKeys<'_> {
    async fn get(&self, id: &str) -> Result<Option<StoredKey>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.settings_table)
            .key("pk", s(&format!("{KEY_PK_PREFIX}{id}")))
            .key("sk", s(KEY_SK))
            .consistent_read(true)
            .send()
            .await
            .map_err(err)?;
        Ok(r.item().and_then(key_of))
    }

    async fn list(&self, team_id: &str) -> Result<Vec<StoredKey>, String> {
        let r = self
            .dynamo
            .query()
            .table_name(self.settings_table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", s(team_id))
            .expression_attribute_values(":p", s(TEAM_SK_PREFIX))
            .scan_index_forward(false)
            .send()
            .await
            .map_err(err)?;
        Ok(r.items().iter().filter_map(key_of).collect())
    }

    async fn create(&self, key: &StoredKey) -> Result<(), String> {
        let data = serde_json::to_string(key).map_err(|e| e.to_string())?;
        let put = |pk: String, sk: String| {
            Put::builder()
                .table_name(self.settings_table)
                .item("pk", s(&pk))
                .item("sk", s(&sk))
                .item("record_type", s("api_key"))
                .item("data", s(&data))
                .condition_expression("attribute_not_exists(pk)")
                .build()
                .map_err(|e| e.to_string())
        };
        self.dynamo
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .put(put(format!("{KEY_PK_PREFIX}{}", key.id), KEY_SK.into())?)
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .put(put(
                        key.team_id.clone(),
                        format!("{TEAM_SK_PREFIX}{}", key.id),
                    )?)
                    .build(),
            )
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn revoke(&self, team_id: &str, id: &str, by: &str, at: &str) -> Result<bool, String> {
        // The checked record must be this team's: a key id from another team
        // matches nothing here.
        let Some(existing) = self.get(id).await? else {
            return Ok(false);
        };
        if existing.team_id != team_id || existing.revoked_at.is_some() {
            return Ok(false);
        }
        let update = |pk: String, sk: String| {
            Update::builder()
                .table_name(self.settings_table)
                .key("pk", s(&pk))
                .key("sk", s(&sk))
                .update_expression("SET revoked_at = :at, revoked_by = :by")
                .condition_expression("attribute_exists(pk) AND attribute_not_exists(revoked_at)")
                .expression_attribute_values(":at", s(at))
                .expression_attribute_values(":by", s(by))
                .build()
                .map_err(|e| e.to_string())
        };
        let out = self
            .dynamo
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .update(update(format!("{KEY_PK_PREFIX}{id}"), KEY_SK.into())?)
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .update(update(
                        team_id.to_string(),
                        format!("{TEAM_SK_PREFIX}{id}"),
                    )?)
                    .build(),
            )
            .send()
            .await;
        match out {
            Ok(_) => Ok(true),
            Err(e) => {
                let text = err(e);
                if text.contains("ConditionalCheckFailed") || text.contains("TransactionCanceled") {
                    Ok(false)
                } else {
                    Err(text)
                }
            }
        }
    }

    async fn count_request(&self, id: &str, window: u64) -> Result<u64, String> {
        let ttl = (window + 2) * RATE_WINDOW_SECS;
        let r = self
            .dynamo
            .update_item()
            .table_name(self.settings_table)
            .key("pk", s(&format!("{KEY_PK_PREFIX}{id}")))
            .key("sk", s(&format!("RATE#{window}")))
            .update_expression("ADD n :one SET #ttl = if_not_exists(#ttl, :ttl)")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
            .send()
            .await
            .map_err(err)?;
        Ok(r.attributes()
            .and_then(|a| a.get("n"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(1))
    }

    async fn note_use(
        &self,
        key: &StoredKey,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), String> {
        let at_s = at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let day = at.format("%Y-%m-%d").to_string();
        let ttl = (at + chrono::Duration::days(365)).timestamp();
        let last = self
            .dynamo
            .update_item()
            .table_name(self.settings_table)
            .key("pk", s(&key.team_id))
            .key("sk", s(&format!("{TEAM_SK_PREFIX}{}", key.id)))
            .update_expression("SET last_used_at = :at")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_values(":at", s(&at_s))
            .send();
        let summary = self
            .dynamo
            .update_item()
            .table_name(self.settings_table)
            .key("pk", s(&key.team_id))
            .key("sk", s(&use_summary_sk(&key.id, &day)))
            .update_expression(
                "ADD minutes :one SET record_type = :rt, #at = :at, actor = :actor, actor_id = :aid, \
                 #action = :action, role_id = :id, role_name = :name, #ttl = :ttl",
            )
            .expression_attribute_names("#at", "at")
            .expression_attribute_names("#action", "action")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":rt", s("role_audit"))
            .expression_attribute_values(":at", s(&at_s))
            .expression_attribute_values(":actor", s(&format!("API key \"{}\"", key.name)))
            .expression_attribute_values(":aid", s(&format!("APIKEY#{}", key.id)))
            .expression_attribute_values(":action", s("api_key_used"))
            .expression_attribute_values(":id", s(&key.id))
            .expression_attribute_values(":name", s(&key.name))
            .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
            .send();
        let (a, b) = tokio::join!(last, summary);
        a.map_err(err)?;
        b.map_err(err)?;
        Ok(())
    }

    async fn maker(&self, team_id: &str, user_id: &str) -> Result<Option<Maker>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.users_table)
            .key("pk", s(team_id))
            .key("sk", s(user_id))
            .send()
            .await
            .map_err(err)?;
        let Some(item) = r.item() else {
            return Ok(None);
        };
        let get = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
        if get("status").as_deref() == Some("invited") {
            return Ok(None);
        }
        Ok(Some(Maker {
            role: get("role").unwrap_or_else(|| "member".into()),
            email: get("email").unwrap_or_default(),
            github_login: get("github_login"),
        }))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Keys kept in memory, the way DynamoKeys keeps them.
    #[derive(Default)]
    pub struct Mem {
        pub keys: Mutex<HashMap<String, StoredKey>>,
        pub counts: Mutex<HashMap<(String, u64), u64>>,
        pub uses: Mutex<Vec<String>>,
        pub members: Mutex<HashMap<(String, String), Maker>>,
    }

    impl KeyStore for Mem {
        async fn get(&self, id: &str) -> Result<Option<StoredKey>, String> {
            Ok(self.keys.lock().unwrap().get(id).cloned())
        }
        async fn list(&self, team_id: &str) -> Result<Vec<StoredKey>, String> {
            let mut v: Vec<StoredKey> = self
                .keys
                .lock()
                .unwrap()
                .values()
                .filter(|k| k.team_id == team_id)
                .cloned()
                .collect();
            v.sort_by(|a, b| b.id.cmp(&a.id));
            Ok(v)
        }
        async fn create(&self, key: &StoredKey) -> Result<(), String> {
            self.keys
                .lock()
                .unwrap()
                .insert(key.id.clone(), key.clone());
            Ok(())
        }
        async fn revoke(
            &self,
            team_id: &str,
            id: &str,
            by: &str,
            at: &str,
        ) -> Result<bool, String> {
            let mut keys = self.keys.lock().unwrap();
            match keys.get_mut(id) {
                Some(k) if k.team_id == team_id && k.revoked_at.is_none() => {
                    k.revoked_at = Some(at.into());
                    k.revoked_by = Some(by.into());
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        async fn count_request(&self, id: &str, window: u64) -> Result<u64, String> {
            let mut c = self.counts.lock().unwrap();
            let n = c.entry((id.to_string(), window)).or_insert(0);
            *n += 1;
            Ok(*n)
        }
        async fn note_use(
            &self,
            key: &StoredKey,
            at: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), String> {
            self.uses.lock().unwrap().push(key.id.clone());
            if let Some(k) = self.keys.lock().unwrap().get_mut(&key.id) {
                k.last_used_at = Some(at.to_rfc3339());
            }
            Ok(())
        }
        async fn maker(&self, team_id: &str, user_id: &str) -> Result<Option<Maker>, String> {
            Ok(self
                .members
                .lock()
                .unwrap()
                .get(&(team_id.to_string(), user_id.to_string()))
                .cloned())
        }
    }

    pub fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-10-24T12:00:30Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    /// A key made by `maker` in `team`, kept in `mem`; returns the token.
    pub fn make_key(mem: &Mem, team: &str, maker: &str, scopes: &[&str], limit: u32) -> String {
        let t = generate();
        let key = StoredKey {
            id: t.id.clone(),
            team_id: team.into(),
            name: "ci".into(),
            hash: t.hash.clone(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            created_by: maker.into(),
            created_by_name: "maker@example.com".into(),
            created_at: now().to_rfc3339(),
            expires_at: None,
            revoked_at: None,
            revoked_by: None,
            last_used_at: None,
            rate_limit_per_minute: limit,
            hint: token_hint(&t.token),
        };
        mem.keys.lock().unwrap().insert(t.id.clone(), key);
        mem.members.lock().unwrap().insert(
            (team.into(), maker.into()),
            Maker {
                role: "admin".into(),
                email: "maker@example.com".into(),
                github_login: None,
            },
        );
        t.token
    }

    async fn accepted(mem: &Mem, token: &str) -> Accepted {
        match authenticate(mem, token, now()).await.unwrap() {
            Outcome::Accepted(a) => *a,
            other => panic!("{other:?}"),
        }
    }

    async fn refused(mem: &Mem, token: &str) -> KeyRefusal {
        match authenticate(mem, token, now()).await.unwrap() {
            Outcome::Refused(r) => r,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_new_key_is_kept_only_as_its_hash() {
        let t = generate();
        assert!(t.token.starts_with("chk_"));
        assert_eq!(token_id(&t.token), Some(t.id.as_str()));
        assert_eq!(t.hash.len(), 64);
        assert!(!t.hash.contains(&t.token[TOKEN_PREFIX.len()..]));
        assert_eq!(t.hash, hash_token(&t.token));
        assert!(hash_matches(&t.token, &t.hash));
        // One character off is a different key.
        let mut wrong = t.token.clone();
        let last = wrong.pop().unwrap();
        wrong.push(if last == 'A' { 'B' } else { 'A' });
        assert!(!hash_matches(&wrong, &t.hash));
        // Two keys never share a secret.
        assert_ne!(generate().token, t.token);
        // The hint shows the start and end, never the middle.
        let hint = token_hint(&t.token);
        assert!(hint.starts_with("chk_") && hint.contains("..."));
        assert!(hint.len() < 20);
    }

    #[test]
    fn malformed_tokens_are_not_keys() {
        for bad in [
            "",
            "chk_",
            "chk_short_secret",
            "sk_01jabcdefghjkmnpqrstvwxyz0_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "chk_01jabcdefghjkmnpqrstvwxyz0_aaaa",
            "chk_01jabcdefghjkmnpqrstvwx!0_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ] {
            assert_eq!(token_id(bad), None, "{bad}");
        }
        let mut h = axum::http::HeaderMap::new();
        h.insert("authorization", "Bearer chk_x".parse().unwrap());
        assert_eq!(bearer_key(&h), Some("chk_x"));
        h.insert("authorization", "bearer   chk_y".parse().unwrap());
        assert_eq!(bearer_key(&h), Some("chk_y"));
        // A session JWT or another scheme is not an API key.
        h.insert("authorization", "Bearer eyJhbGciOi".parse().unwrap());
        assert_eq!(bearer_key(&h), None);
        h.insert("authorization", "Basic chk_x".parse().unwrap());
        assert_eq!(bearer_key(&h), None);
    }

    #[tokio::test]
    async fn a_key_authenticates_as_its_own_team_only() {
        let mem = Mem::default();
        let a = make_key(&mem, "TEAM#a", "USER#alice", &["runs"], 100);
        let b = make_key(&mem, "TEAM#b", "USER#bob", &["plans"], 100);
        let got = accepted(&mem, &a).await;
        assert_eq!(got.key.team_id, "TEAM#a");
        assert_eq!(accepted(&mem, &b).await.key.team_id, "TEAM#b");
        // Team b's id with team a's secret is nobody.
        let a_secret = a[TOKEN_PREFIX.len()..].split_once('_').unwrap().1;
        let forged = format!("chk_{}_{a_secret}", token_id(&b).unwrap());
        assert_eq!(refused(&mem, &forged).await, KeyRefusal::Invalid);
        // A team can't revoke or list another team's key.
        let b_id = token_id(&b).unwrap().to_string();
        assert!(!mem
            .revoke("TEAM#a", &b_id, "USER#alice", "now")
            .await
            .unwrap());
        assert!(mem
            .list("TEAM#a")
            .await
            .unwrap()
            .iter()
            .all(|k| k.team_id == "TEAM#a"));
        assert_eq!(accepted(&mem, &b).await.key.team_id, "TEAM#b");
    }

    #[tokio::test]
    async fn revoked_and_expired_keys_are_refused() {
        let mem = Mem::default();
        let token = make_key(&mem, "TEAM#a", "USER#alice", &[], 100);
        let id = token_id(&token).unwrap().to_string();
        accepted(&mem, &token).await;
        assert!(mem
            .revoke("TEAM#a", &id, "USER#alice", "now")
            .await
            .unwrap());
        assert_eq!(refused(&mem, &token).await, KeyRefusal::Revoked);
        // Revoking twice finds nothing to revoke.
        assert!(!mem
            .revoke("TEAM#a", &id, "USER#alice", "now")
            .await
            .unwrap());

        let token = make_key(&mem, "TEAM#a", "USER#alice", &[], 100);
        let id = token_id(&token).unwrap().to_string();
        mem.keys.lock().unwrap().get_mut(&id).unwrap().expires_at =
            Some("2026-10-24T12:00:00Z".into());
        assert_eq!(refused(&mem, &token).await, KeyRefusal::Expired);
        mem.keys.lock().unwrap().get_mut(&id).unwrap().expires_at =
            Some("2026-10-25T00:00:00Z".into());
        accepted(&mem, &token).await;
    }

    #[tokio::test]
    async fn a_key_stops_when_its_maker_leaves() {
        let mem = Mem::default();
        let token = make_key(&mem, "TEAM#a", "USER#alice", &["runs"], 100);
        accepted(&mem, &token).await;
        mem.members.lock().unwrap().clear();
        assert_eq!(refused(&mem, &token).await, KeyRefusal::MakerGone);
    }

    #[tokio::test]
    async fn each_key_has_its_own_rate_limit() {
        let mem = Mem::default();
        let a = make_key(&mem, "TEAM#a", "USER#alice", &[], 3);
        let b = make_key(&mem, "TEAM#a", "USER#alice", &[], 3);
        for want in [2, 1, 0] {
            assert_eq!(accepted(&mem, &a).await.rate.remaining, want);
        }
        match authenticate(&mem, &a, now()).await.unwrap() {
            Outcome::RateLimited(w) => {
                assert!(!w.allowed);
                assert_eq!(w.limit, 3);
                assert_eq!(w.remaining, 0);
                assert_eq!(w.reset_secs, 30);
            }
            other => panic!("{other:?}"),
        }
        // Another key of the same team is untouched.
        assert_eq!(accepted(&mem, &b).await.rate.remaining, 2);
        // The next minute starts over.
        let later = now() + chrono::Duration::seconds(60);
        assert!(matches!(
            authenticate(&mem, &a, later).await.unwrap(),
            Outcome::Accepted(_)
        ));
        // Use is noted once per key per minute, not per request.
        assert_eq!(mem.uses.lock().unwrap().len(), 3);
    }

    #[test]
    fn the_rate_window_counts_down_and_resets() {
        let w = rate_decision(1, 600, 120);
        assert!(w.allowed);
        assert_eq!((w.limit, w.remaining, w.reset_secs), (600, 599, 60));
        let w = rate_decision(601, 600, 179);
        assert!(!w.allowed);
        assert_eq!((w.remaining, w.reset_secs), (0, 1));
        assert_eq!(rate_window(119), 1);
        assert_eq!(rate_window(120), 2);
    }

    #[test]
    fn scopes_are_the_public_capabilities_and_never_more_than_the_maker_holds() {
        let keys: Vec<&str> = key_scopes().iter().map(|c| c.key).collect();
        assert!(keys.contains(&"view") && keys.contains(&"runs") && keys.contains(&"plans"));
        // Nothing that manages people, roles, keys or integrations.
        for never in [
            "people",
            "roles",
            "api_keys",
            "integrations",
            "reset",
            "support",
        ] {
            assert!(!keys.contains(&never), "{never}");
        }

        let admin = Holder::builtin("admin");
        let got = checked_scopes(&admin, &["runs".into()]).unwrap();
        assert_eq!(got, BTreeSet::from(["view", "runs"]));
        assert!(matches!(
            checked_scopes(&admin, &["people".into()]),
            Err(ScopeError::NotForKeys(_))
        ));
        assert!(matches!(
            checked_scopes(&admin, &["superpowers".into()]),
            Err(ScopeError::Unknown(_))
        ));
        // Someone whose custom role holds key management but not runs can't
        // give a key runs.
        let caps = vec!["api_keys".to_string(), "plans".to_string()];
        let keeper = Holder::custom("r1", Some(("Key keeper", &caps)));
        match checked_scopes(&keeper, &["runs".into()]) {
            Err(ScopeError::MoreThanYouHave(c)) => assert_eq!(c.key, "runs"),
            other => panic!("{other:?}"),
        }
        assert!(checked_scopes(&keeper, &["plans".into()]).is_ok());
    }

    #[test]
    fn a_key_holds_its_scopes_cut_to_its_maker_and_resolves_like_a_role() {
        use crate::permissions::{resolve_access, Access};
        let key = StoredKey {
            id: "k1".into(),
            team_id: "TEAM#a".into(),
            name: "ci".into(),
            hash: String::new(),
            scopes: vec![
                "view".into(),
                "runs".into(),
                "plans".into(),
                "people".into(),
            ],
            created_by: "USER#a".into(),
            created_by_name: String::new(),
            created_at: String::new(),
            expires_at: None,
            revoked_at: None,
            revoked_by: None,
            last_used_at: None,
            rate_limit_per_minute: 600,
            hint: String::new(),
        };
        // A stored scope that is not a key scope is never held.
        let h = key_holder(&key, &Holder::builtin("owner"));
        assert_eq!(h.caps, BTreeSet::from(["view", "runs", "plans"]));
        assert!(!h.manages_roles());
        assert_eq!(
            resolve_access(&h, Some("api::retry_run")),
            Access::Allow {
                effective_role: "member"
            }
        );
        assert!(matches!(
            resolve_access(&h, Some("api::sync_repos")),
            Access::Deny { .. }
        ));

        // The maker was made a viewer since: the key can only read now.
        let h = key_holder(&key, &Holder::builtin("viewer"));
        assert_eq!(h.caps, BTreeSet::from(["view"]));
        match resolve_access(&h, Some("api::retry_run")) {
            Access::Deny { capability } => assert_eq!(capability.key, "runs"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            resolve_access(&h, Some("api::list_runs")),
            Access::Allow { .. }
        ));
    }

    #[test]
    fn the_use_summary_is_one_entry_a_day() {
        assert_eq!(
            use_summary_sk("k1", "2026-10-24"),
            "ROLE_AUDIT#2026-10-24T23:59:59.999Z#KEY_USE#k1"
        );
        assert_eq!(
            use_summary_detail(3, "2026-10-24T14:05:09.000Z"),
            "Used during 3 minutes today, last at 14:05 UTC."
        );
        assert_eq!(
            use_summary_detail(1, "2026-10-24T14:05:09.000Z"),
            "Used during 1 minute today, last at 14:05 UTC."
        );
    }
}
