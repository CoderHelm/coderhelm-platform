//! The install side of hybrid: its registration with the hosted platform,
//! signed calls to it, and GitHub installation tokens from the hosted broker.
//!
//! The gateway and the worker set the credentials once at start (from the
//! install's secret, [`set_credentials`]); every caller then asks here. When
//! no credentials are set, the install isn't hybrid and nothing here is used.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

use serde_json::Value;

use super::sign;

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
    let mut req = http
        .request(m, &url)
        .timeout(std::time::Duration::from_secs(8))
        .header(sign::INSTALL_HEADER, &creds.install_id)
        .header(sign::TIMESTAMP_HEADER, ts.to_string())
        .header(sign::SIGNATURE_HEADER, sig)
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
    let v: Value = resp.json().await.unwrap_or(Value::Null);
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
