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
    let sig = sign::sign_request(&creds.secret, ts, method, path, &bytes);
    let url = format!("{}{path}", super::hosted_url());
    let m = reqwest::Method::from_bytes(method.as_bytes()).unwrap_or(reqwest::Method::GET);
    let request_id = hosted_sig::new_request_id();
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
    check_answer(
        hosted_sig::trusted(),
        &creds.install_id,
        &request_id,
        status,
        method,
        path,
        &headers,
        &raw,
        chrono::Utc::now().timestamp(),
    )?;
    let v: Value = serde_json::from_slice(&raw).unwrap_or(Value::Null);
    if (200..300).contains(&status) {
        return Ok(v);
    }
    Err(CallError {
        status: Some(status),
        code: v["code"].as_str().unwrap_or("broker_refused").to_string(),
        message: v["error"]
            .as_str()
            .unwrap_or("CoderHelm's servers refused the request.")
            .to_string(),
    })
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
}

static TOKENS: Mutex<Option<HashMap<u64, Cached>>> = Mutex::new(None);
static BOT_LOGIN: RwLock<Option<String>> = RwLock::new(None);

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
    let creds = credentials().ok_or_else(|| CallError {
        status: None,
        code: "broker_not_linked".into(),
        message: "This install isn't registered with CoderHelm's apps.".into(),
    })?;
    let v = call(
        http,
        &creds,
        "POST",
        "/hybrid/github/token",
        Some(&serde_json::json!({ "installation_id": installation_id })),
    )
    .await?;
    let token = v["token"].as_str().unwrap_or_default().to_string();
    if token.is_empty() {
        return Err(CallError {
            status: None,
            code: "broker_refused".into(),
            message: "The broker answered without a token.".into(),
        });
    }
    let expires_at = v["expires_at"]
        .as_str()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.timestamp())
        .unwrap_or(now + 3600);
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
