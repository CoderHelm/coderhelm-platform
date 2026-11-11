//! The install side of hybrid: its registration with the hosted platform,
//! signed calls to it, and GitHub installation tokens from the hosted broker.
//!
//! The gateway and the worker set the credentials once at start (from the
//! install's secret, [`set_credentials`]); every caller then asks here. When
//! no credentials are set, the install isn't hybrid and nothing here is used.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

use serde_json::Value;

use super::{hosted_sig, sign};

/// What an install holds after registering: its id, its registration secret
/// and the key the hosted router signs relayed events with.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub install_id: String,
    pub secret: String,
    pub relay_key: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("install_id", &self.install_id)
            .finish_non_exhaustive()
    }
}

impl Credentials {
    /// From the install's secret fields, when all three are there.
    pub fn from_parts(id: Option<&str>, secret: Option<&str>, relay: Option<&str>) -> Option<Self> {
        let t = |v: Option<&str>| {
            v.map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string)
        };
        Some(Self {
            install_id: t(id).filter(|i| sign::is_install_id(i))?,
            secret: t(secret)?,
            relay_key: t(relay)?,
        })
    }
}

static CREDS: RwLock<Option<Credentials>> = RwLock::new(None);

pub fn set_credentials(c: Option<Credentials>) {
    if let Ok(mut g) = CREDS.write() {
        *g = c;
    }
}

pub fn credentials() -> Option<Credentials> {
    CREDS.read().ok().and_then(|g| g.clone())
}

/// A failed call to the hosted side, with the reason code the Deliveries
/// page explains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallError {
    pub status: Option<u16>,
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

impl std::error::Error for CallError {}

/// CoderHelm's servers failed (a 5xx or 429 that carries no reason, such as
/// a gateway that couldn't start) or never answered: an outage on
/// CoderHelm's side, not a refusal of this install.
pub const UNAVAILABLE: &str = "broker_unavailable";

impl CallError {
    /// An outage on CoderHelm's side ([`UNAVAILABLE`], or no answer at all):
    /// asking again later works without anyone changing anything.
    pub fn is_outage(&self) -> bool {
        matches!(self.code.as_str(), UNAVAILABLE | "broker_unreachable")
    }
}

/// The error for an answer outside 2xx. CoderHelm's own refusals carry a
/// `code` and an `error` sentence. An answer without them came from in front
/// of CoderHelm's handlers (its API gateway answering for a function that
/// failed, or a proxy): a 5xx or 429 is an outage, anything else a refusal
/// that names its HTTP status, never a bare "refused".
pub fn answer_error(status: u16, v: &Value) -> CallError {
    if let Some(code) = v["code"].as_str().filter(|c| !c.is_empty()) {
        return CallError {
            status: Some(status),
            code: code.to_string(),
            message: v["error"]
                .as_str()
                .unwrap_or("CoderHelm's servers refused the request.")
                .to_string(),
        };
    }
    if status >= 500 || status == 429 {
        return CallError {
            status: Some(status),
            code: UNAVAILABLE.into(),
            message: format!(
                "CoderHelm's servers failed to answer (HTTP {status}): an outage on CoderHelm's \
                 side, not a refusal of this install. CoderHelm asks again by itself."
            ),
        };
    }
    CallError {
        status: Some(status),
        code: "broker_refused".into(),
        message: format!(
            "CoderHelm's servers refused the request (HTTP {status}) without saying why."
        ),
    }
}

/// A signed request to the hosted platform. `path` starts with `/hybrid/`.
pub async fn call(
    http: &reqwest::Client,
    creds: &Credentials,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<Value, CallError> {
    let bytes = body
        .map(|b| serde_json::to_vec(b).unwrap_or_default())
        .unwrap_or_default();
    let ts = chrono::Utc::now().timestamp();
    let request_id = hosted_sig::new_request_id();
    let sig = sign::sign_request_v2(&creds.secret, ts, &request_id, method, path, &bytes);
    let url = format!("{}{path}", super::hosted_url());
    let m = reqwest::Method::from_bytes(method.as_bytes()).unwrap_or(reqwest::Method::GET);
    let mut req = http
        .request(m, &url)
        .timeout(std::time::Duration::from_secs(8))
        .header(sign::INSTALL_HEADER, &creds.install_id)
        .header(sign::TIMESTAMP_HEADER, ts.to_string())
        .header(sign::SIGNATURE_HEADER, sig)
        .header(hosted_sig::REQUEST_ID_HEADER, &request_id)
        .header("user-agent", "coderhelm-selfhost");
    if body.is_some() {
        req = req.header("content-type", "application/json").body(bytes);
    }
    let resp = req.send().await.map_err(|e| CallError {
        status: None,
        code: "broker_unreachable".into(),
        message: format!("CoderHelm's servers didn't answer: {}", e.without_url()),
    })?;
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let raw = resp.bytes().await.unwrap_or_default();
    let signed = check_answer(
        hosted_sig::trusted(),
        &creds.install_id,
        &request_id,
        status,
        method,
        path,
        &headers,
        &raw,
        chrono::Utc::now().timestamp(),
    );
    let v: Value = serde_json::from_slice(&raw).unwrap_or(Value::Null);
    if let Err(e) = signed {
        // A failing gateway in front of CoderHelm answers 5xx unsigned: that
        // is an outage, and nothing in it is used either way.
        let out = answer_error(status, &Value::Null);
        return Err(if out.is_outage() { out } else { e });
    }
    if (200..300).contains(&status) {
        return Ok(v);
    }
    Err(answer_error(status, &v))
}

/// Checks that an answer from CoderHelm's servers carries CoderHelm's
/// signature for this call, once this release pins CoderHelm's keys (until
/// then TLS to the hosted address is the check). `install_id` is empty for
/// the calls made before the install has one (registration, a name lookup).
#[allow(clippy::too_many_arguments)]
pub fn check_answer(
    keys: &hosted_sig::TrustedKeys,
    install_id: &str,
    request_id: &str,
    status: u16,
    method: &str,
    path: &str,
    headers: &reqwest::header::HeaderMap,
    body: &[u8],
    now: i64,
) -> Result<(), CallError> {
    if keys.is_empty() {
        return Ok(());
    }
    let refused = |p: sign::SignatureProblem| {
        tracing::warn!(
            code = p.code(),
            path,
            "hybrid: an answer not signed by CoderHelm was refused"
        );
        CallError {
            status: Some(status),
            code: "not_signed_by_coderhelm".into(),
            message: format!("Refused: not signed by CoderHelm. {}", p.message()),
        }
    };
    let stamp = keys
        .verify(
            headers
                .get(hosted_sig::HOSTED_HEADER)
                .and_then(|v| v.to_str().ok()),
            install_id,
            hosted_sig::RESPONSE_SOURCE,
            &hosted_sig::response_event(status, method, path),
            body,
            now,
        )
        .map_err(refused)?;
    if stamp.delivery_id != request_id {
        return Err(refused(sign::SignatureProblem::Mismatch));
    }
    Ok(())
}

#[derive(Clone)]
struct Cached {
    token: String,
    expires_at: i64,
    grant: TokenGrant,
}

/// What the broker's last token for an installation carries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TokenGrant {
    /// Permission name to level, as minted.
    pub permissions: std::collections::BTreeMap<String, String>,
    /// Permissions CoderHelm's app carries that the installation hasn't
    /// granted (its owner hasn't accepted them yet).
    pub withheld: Vec<String>,
    /// Whether the broker read the installation's permissions (false: an
    /// older broker, or GitHub couldn't say).
    pub installation_known: bool,
}

impl TokenGrant {
    /// Reads the broker's token answer.
    pub fn from_answer(v: &serde_json::Value) -> Self {
        TokenGrant {
            permissions: crate::hybrid::broker::granted_permissions(v),
            withheld: v["withheld"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|w| w.as_str().map(String::from))
                .collect(),
            installation_known: v["installation_permissions_known"].as_bool() == Some(true),
        }
    }
}

/// The permissions of the broker's current token for an installation, once
/// one was minted.
pub fn token_grant(installation_id: u64) -> Option<TokenGrant> {
    TOKENS.lock().ok().and_then(|g| {
        g.as_ref()
            .and_then(|m| m.get(&installation_id).map(|c| c.grant.clone()))
    })
}

static TOKENS: Mutex<Option<HashMap<u64, Cached>>> = Mutex::new(None);
static BOT_LOGIN: RwLock<Option<String>> = RwLock::new(None);

/// An installation the broker gave no token for: asked again only after
/// `until`, so sweeps over many repositories ask once, not once a repository
/// a tick.
#[derive(Debug, Clone)]
struct Failing {
    error: CallError,
    failures: u32,
    until: i64,
}

static FAILING: Mutex<Option<HashMap<u64, Failing>>> = Mutex::new(None);

/// How long to wait before asking the broker again after `failures` failures
/// in a row: an outage from 30 seconds doubling to 10 minutes, a refusal
/// (which needs someone to change something) from 5 minutes to an hour.
pub fn retry_after_secs(e: &CallError, failures: u32) -> i64 {
    let (first, most) = if e.is_outage() {
        (30, 600)
    } else {
        (300, 3600)
    };
    (first << failures.saturating_sub(1).min(10)).min(most)
}

/// The broker's last refusal for an installation while it is not asked
/// again, with when it will be. None when a token may be asked for now.
pub fn token_backoff(installation_id: u64) -> Option<CallError> {
    token_backoff_at(installation_id, chrono::Utc::now().timestamp())
}

fn token_backoff_at(installation_id: u64, now: i64) -> Option<CallError> {
    let f = FAILING
        .lock()
        .ok()?
        .as_ref()?
        .get(&installation_id)
        .cloned()?;
    if now >= f.until {
        return None;
    }
    let at = chrono::DateTime::from_timestamp(f.until, 0)
        .map(|t| t.format("%H:%M UTC").to_string())
        .unwrap_or_default();
    let mut e = f.error;
    e.message = format!("{} CoderHelm asks again at {at}.", e.message.trim_end());
    Some(e)
}

/// Records the broker's answer for an installation: a failure starts or
/// extends its wait (logged once, when it starts or its reason changes), a
/// token ends it.
fn note_token_answer(installation_id: u64, answer: Result<(), &CallError>, now: i64) {
    let Ok(mut g) = FAILING.lock() else {
        return;
    };
    let all = g.get_or_insert_with(HashMap::new);
    match answer {
        Ok(()) => {
            if let Some(f) = all.remove(&installation_id) {
                tracing::info!(
                    installation_id,
                    failures = f.failures,
                    "hybrid: CoderHelm's broker gives tokens again"
                );
            }
        }
        Err(e) => {
            let prev = all.get(&installation_id);
            let same = prev.is_some_and(|p| p.error.code == e.code);
            let failures = if same {
                prev.map_or(1, |p| p.failures + 1)
            } else {
                1
            };
            let wait = retry_after_secs(e, failures);
            if !same {
                tracing::warn!(
                    installation_id,
                    code = %e.code,
                    status = ?e.status,
                    retry_in_secs = wait,
                    error = %e.message,
                    "hybrid: no GitHub token from CoderHelm's broker; asking again later"
                );
            }
            all.insert(
                installation_id,
                Failing {
                    error: e.clone(),
                    failures,
                    until: now + wait,
                },
            );
        }
    }
}

/// Forgets an installation's wait, so the next ask goes to the broker (a
/// person re-checking the Status page after fixing the cause).
pub fn forget_token_backoff(installation_id: u64) {
    if let Ok(mut g) = FAILING.lock() {
        if let Some(all) = g.as_mut() {
            all.remove(&installation_id);
        }
    }
}

/// A GitHub installation token from the hosted broker (cached until five
/// minutes before it expires).
pub async fn github_token(
    http: &reqwest::Client,
    installation_id: u64,
) -> Result<String, CallError> {
    let now = chrono::Utc::now().timestamp();
    if let Some(c) = TOKENS
        .lock()
        .ok()
        .and_then(|g| g.as_ref().and_then(|m| m.get(&installation_id).cloned()))
    {
        if now < c.expires_at - 300 {
            return Ok(c.token);
        }
    }
    if let Some(e) = token_backoff_at(installation_id, now) {
        return Err(e);
    }
    let creds = credentials().ok_or_else(|| CallError {
        status: None,
        code: "broker_not_linked".into(),
        message: "This install isn't registered with CoderHelm's apps.".into(),
    })?;
    let answer = call(
        http,
        &creds,
        "POST",
        "/hybrid/github/token",
        Some(&serde_json::json!({ "installation_id": installation_id })),
    )
    .await
    .and_then(|v| {
        if v["token"].as_str().unwrap_or_default().is_empty() {
            Err(CallError {
                status: None,
                code: "broker_refused".into(),
                message: "The broker answered without a token.".into(),
            })
        } else {
            Ok(v)
        }
    });
    note_token_answer(installation_id, answer.as_ref().map(|_| ()), now);
    let v = answer?;
    let token = v["token"].as_str().unwrap_or_default().to_string();
    let expires_at = v["expires_at"]
        .as_str()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.timestamp())
        .unwrap_or(now + 3600);
    crate::github_grants::record(installation_id, &v["permissions"]);
    if let Some(login) = v["bot_login"].as_str().filter(|l| !l.is_empty()) {
        if let Ok(mut g) = BOT_LOGIN.write() {
            *g = Some(login.to_string());
        }
    }
    if let Ok(mut g) = TOKENS.lock() {
        g.get_or_insert_with(HashMap::new).insert(
            installation_id,
            Cached {
                token: token.clone(),
                expires_at,
                grant: TokenGrant::from_answer(&v),
            },
        );
    }
    Ok(token)
}

/// The hosted app's bot login (`coderhelm[bot]`), once a token was minted.
pub fn bot_login() -> Option<String> {
    BOT_LOGIN.read().ok().and_then(|g| g.clone())
}

/// Whether GitHub goes through the hosted broker: registered, and no GitHub
/// App key of its own.
pub fn github_via_broker(own_private_key: &str) -> bool {
    own_private_key.trim().is_empty() && credentials().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_brokers_answer_says_what_the_token_carries_and_lacks() {
        let g = TokenGrant::from_answer(&serde_json::json!({
            "token": "t",
            "permissions": {"contents": "write", "metadata": "read"},
            "withheld": ["vulnerability_alerts"],
            "installation_permissions_known": true,
        }));
        assert_eq!(
            g.permissions.get("contents").map(String::as_str),
            Some("write")
        );
        assert_eq!(g.withheld, vec!["vulnerability_alerts"]);
        assert!(g.installation_known);
        // An older broker says neither.
        let old = TokenGrant::from_answer(&serde_json::json!({"token": "t"}));
        assert!(old.withheld.is_empty() && !old.installation_known);
    }

    /// 11/11: CoderHelm's hosted gateway couldn't start for five and a half
    /// hours, and its API gateway answered every call `502
    /// {"message":"Internal Server Error"}`. That read as "CoderHelm's servers
    /// refused the request (broker_refused)": a refusal of this install. It
    /// is an outage, named as one.
    #[test]
    fn an_answer_without_coderhelms_reason_is_an_outage_or_names_its_status() {
        let gw = serde_json::json!({"message": "Internal Server Error"});
        for status in [500, 502, 503, 504, 429] {
            let e = answer_error(status, &gw);
            assert_eq!(e.code, UNAVAILABLE, "{status}");
            assert!(e.is_outage());
            assert!(
                e.message.contains(&format!("HTTP {status}")),
                "{}",
                e.message
            );
            assert!(!e.message.contains("refused"), "{}", e.message);
        }
        let e = answer_error(403, &serde_json::json!({"message": "Forbidden"}));
        assert_eq!(e.code, "broker_refused");
        assert!(
            e.message.contains("HTTP 403") && !e.is_outage(),
            "{}",
            e.message
        );
        assert!(answer_error(502, &Value::Null).is_outage());
        // CoderHelm's own refusal keeps its code and sentence, whatever the status.
        let own = answer_error(
            403,
            &serde_json::json!({"code": "broker_not_linked", "error": "That GitHub installation isn't linked to this install."}),
        );
        assert_eq!(own.code, "broker_not_linked");
        assert_eq!(
            own.message,
            "That GitHub installation isn't linked to this install."
        );
        let mint = answer_error(
            502,
            &serde_json::json!({"code": "broker_refused", "error": "GitHub refused the token: 422"}),
        );
        assert_eq!(mint.code, "broker_refused");
        assert!(!mint.is_outage());
        assert!(CallError {
            status: None,
            code: "broker_unreachable".into(),
            message: String::new()
        }
        .is_outage());
    }

    /// One installation the broker can't serve is asked once, then again
    /// after a wait that grows, not once a repository each sweep: the
    /// sweeps' 282 failed asks in a day and a half become a handful.
    #[test]
    fn a_failed_token_is_asked_for_again_only_after_a_growing_wait() {
        let iid = 9_100_001;
        let down = answer_error(502, &Value::Null);
        assert!(token_backoff_at(iid, 1_000).is_none());
        note_token_answer(iid, Err(&down), 1_000);
        let e = token_backoff_at(iid, 1_000).expect("waits");
        assert_eq!(e.code, UNAVAILABLE);
        assert!(e.message.contains("asks again at"), "{}", e.message);
        assert!(token_backoff_at(iid, 1_029).is_some());
        assert!(
            token_backoff_at(iid, 1_030).is_none(),
            "30 s after the first outage"
        );
        note_token_answer(iid, Err(&down), 1_030);
        assert!(token_backoff_at(iid, 1_089).is_some(), "then 60 s");
        assert!(token_backoff_at(iid, 1_090).is_none());
        // Outages wait at most 10 minutes; refusals 5 minutes to an hour.
        assert_eq!(retry_after_secs(&down, 1), 30);
        assert_eq!(retry_after_secs(&down, 30), 600);
        let refused = answer_error(
            403,
            &serde_json::json!({"code": "broker_not_linked", "error": "x"}),
        );
        assert_eq!(retry_after_secs(&refused, 1), 300);
        assert_eq!(retry_after_secs(&refused, 2), 600);
        assert_eq!(retry_after_secs(&refused, 30), 3600);
        // Another reason starts its own wait; a token ends it.
        note_token_answer(iid, Err(&refused), 1_090);
        assert_eq!(
            token_backoff_at(iid, 1_100).unwrap().code,
            "broker_not_linked"
        );
        assert!(token_backoff_at(iid, 1_389).is_some() && token_backoff_at(iid, 1_390).is_none());
        note_token_answer(iid, Ok(()), 1_400);
        assert!(token_backoff_at(iid, 1_400).is_none());
        // Other installations are not held by this one.
        note_token_answer(iid, Err(&refused), 2_000);
        assert!(token_backoff_at(iid + 1, 2_000).is_none());
        forget_token_backoff(iid);
        assert!(token_backoff_at(iid, 2_000).is_none());
    }

    #[test]
    fn credentials_need_all_three_parts_and_a_real_id() {
        let id = "ins_abcdefghijklmnopqrstuvwx";
        assert!(Credentials::from_parts(Some(id), Some("s"), Some("r")).is_some());
        assert!(Credentials::from_parts(Some(id), Some(" "), Some("r")).is_none());
        assert!(Credentials::from_parts(Some("nope"), Some("s"), Some("r")).is_none());
        assert!(Credentials::from_parts(None, Some("s"), Some("r")).is_none());
        let c = Credentials::from_parts(Some(id), Some("secret-value"), Some("r")).unwrap();
        assert!(!format!("{c:?}").contains("secret-value"));
    }

    fn signed_answer(
        install: &str,
        request_id: &str,
        status: u16,
        path: &str,
        body: &[u8],
    ) -> (hosted_sig::TrustedKeys, reqwest::header::HeaderMap) {
        use base64::Engine;
        use ed25519_dalek::{Signer, SigningKey};
        let k = SigningKey::from_bytes(&[5; 32]);
        let pub_b64 =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(k.verifying_key().to_bytes());
        let kid = hosted_sig::kid_of(&k.verifying_key().to_bytes());
        let event = hosted_sig::response_event(status, "POST", path);
        let m = hosted_sig::Message {
            install_id: install,
            ts: 1_000,
            delivery_id: request_id,
            source: hosted_sig::RESPONSE_SOURCE,
            event: &event,
            body,
        };
        let sig = k.sign(&hosted_sig::canonical(&kid, &m).unwrap());
        let mut h = reqwest::header::HeaderMap::new();
        h.insert(
            hosted_sig::HOSTED_HEADER,
            hosted_sig::header(&kid, &m, &sig.to_bytes())
                .parse()
                .unwrap(),
        );
        (hosted_sig::TrustedKeys::parse(&pub_b64).unwrap(), h)
    }

    #[test]
    fn a_signed_answer_to_this_call_is_accepted() {
        let id = "ins_abcdefghijklmnopqrstuvwx";
        let (keys, h) = signed_answer(
            id,
            "req_1",
            200,
            "/hybrid/github/token",
            b"{\"token\":\"t\"}",
        );
        assert!(check_answer(
            &keys,
            id,
            "req_1",
            200,
            "POST",
            "/hybrid/github/token",
            &h,
            b"{\"token\":\"t\"}",
            1_000
        )
        .is_ok());
    }

    #[test]
    fn an_unsigned_or_altered_answer_is_refused_once_keys_are_pinned() {
        let id = "ins_abcdefghijklmnopqrstuvwx";
        let (keys, h) = signed_answer(
            id,
            "req_1",
            200,
            "/hybrid/github/token",
            b"{\"token\":\"t\"}",
        );
        let none = reqwest::header::HeaderMap::new();
        let e = check_answer(
            &keys,
            id,
            "req_1",
            200,
            "POST",
            "/hybrid/github/token",
            &none,
            b"{}",
            1_000,
        )
        .unwrap_err();
        assert_eq!(e.code, "not_signed_by_coderhelm");
        // Another token in the body, another call's id, another path, another status.
        assert!(check_answer(
            &keys,
            id,
            "req_1",
            200,
            "POST",
            "/hybrid/github/token",
            &h,
            b"{\"token\":\"evil\"}",
            1_000
        )
        .is_err());
        assert!(check_answer(
            &keys,
            id,
            "req_2",
            200,
            "POST",
            "/hybrid/github/token",
            &h,
            b"{\"token\":\"t\"}",
            1_000
        )
        .is_err());
        assert!(check_answer(
            &keys,
            id,
            "req_1",
            200,
            "POST",
            "/hybrid/names",
            &h,
            b"{\"token\":\"t\"}",
            1_000
        )
        .is_err());
        assert!(check_answer(
            &keys,
            id,
            "req_1",
            500,
            "POST",
            "/hybrid/github/token",
            &h,
            b"{\"token\":\"t\"}",
            1_000
        )
        .is_err());
        // Without pinned keys the answer passes (TLS is the check then).
        assert!(check_answer(
            &hosted_sig::TrustedKeys::default(),
            id,
            "req_1",
            200,
            "POST",
            "/x",
            &none,
            b"{}",
            1_000
        )
        .is_ok());
    }

    #[test]
    fn the_broker_is_used_only_without_an_own_app_key() {
        set_credentials(None);
        assert!(!github_via_broker(""));
        set_credentials(Credentials::from_parts(
            Some("ins_abcdefghijklmnopqrstuvwx"),
            Some("s"),
            Some("r"),
        ));
        assert!(github_via_broker(""));
        assert!(!github_via_broker("-----BEGIN RSA PRIVATE KEY-----"));
        set_credentials(None);
    }
}
