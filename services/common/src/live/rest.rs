//! A team's REST API, read only: GET requests under the paths the team
//! allows, with its key sent in the header the API expects.
//!
//! Every request is built here: the path must start with one of the team's
//! allowed prefixes and can't climb out of it (`..`, `//`, a scheme or a host
//! in the path are refused), the query is plain key/value pairs, and only GET
//! is ever sent. Results go through the same redaction as every other source:
//! hidden fields by name, and emails, card numbers and keys in text.

use serde_json::Value;

use super::connectors::{clip, Connector};
use super::http::{Method, Request, Transport};
use super::redact::{hidden_terms, redact_all, redact_text, DEFAULT_HIDDEN};
use super::StepError;

/// Characters of a response an agent reads.
pub const MAX_CHARS: usize = 30_000;
/// Path prefixes a team may allow.
pub const MAX_PATHS: usize = 20;

fn refused(message: impl Into<String>, fix: &str) -> StepError {
    StepError::new("read_only", message, fix)
}

/// Accepts `https://host` (an optional port), no path, query or credentials.
pub fn check_base(raw: &str) -> Result<String, StepError> {
    let fix = "Paste the API's base address, like https://api.example.com";
    let url = reqwest::Url::parse(raw.trim())
        .map_err(|_| StepError::new("reach", "That isn't a web address.", fix))?;
    let host = url.host_str().unwrap_or("").to_ascii_lowercase();
    let ok = url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && matches!(url.path(), "" | "/")
        && host.contains('.')
        && host.parse::<std::net::IpAddr>().is_err();
    if !ok {
        return Err(StepError::new(
            "reach",
            "That isn't an https API address.",
            fix,
        ));
    }
    Ok(match url.port() {
        Some(p) => format!("https://{host}:{p}"),
        None => format!("https://{host}"),
    })
}

/// A header name the key is sent in: letters, digits and `-`.
pub fn check_header(raw: &str) -> Result<String, StepError> {
    let h = raw.trim();
    let ok = !h.is_empty()
        && h.len() <= 64
        && h.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        && !matches!(
            h.to_ascii_lowercase().as_str(),
            "host" | "cookie" | "content-length" | "transfer-encoding" | "connection"
        );
    if ok {
        Ok(h.to_string())
    } else {
        Err(refused(
            format!("{h} isn't a header the key can go in."),
            "Use the header the API documents, like X-Api-Key or Authorization.",
        ))
    }
}

/// A path as it may be requested: starts with `/`, plain URL path characters,
/// nothing that climbs out (`..`, `//`, `\`), no query or fragment (pass the
/// query separately).
pub fn check_path(raw: &str) -> Result<String, StepError> {
    let p = raw.trim();
    let ok = p.starts_with('/')
        && p.len() <= 500
        && !p.contains("..")
        && !p.contains("//")
        && p.chars()
            .all(|c| c.is_ascii_alphanumeric() || "/-_.~:@%,+=".contains(c));
    if ok {
        Ok(p.to_string())
    } else {
        Err(refused(
            format!("{p} isn't a path that can be requested."),
            "Pass a path like /v1/items/42, without the address, query or `..`.",
        ))
    }
}

/// Whether `path` sits under one of the team's allowed prefixes, at a
/// segment boundary (`/v1/orders` allows `/v1/orders/7`, not `/v1/ordersx`).
pub fn path_allowed(path: &str, allowed: &[String]) -> bool {
    allowed.iter().any(|a| {
        let a = a.trim_end_matches('/');
        path == a
            || path
                .strip_prefix(a)
                .is_some_and(|rest| rest.starts_with('/'))
    })
}

/// The query string for `query` (an object of strings, numbers or booleans).
pub fn query_string(query: Option<&Value>) -> Result<String, StepError> {
    let Some(q) = query.filter(|q| !q.is_null()) else {
        return Ok(String::new());
    };
    let Some(map) = q.as_object() else {
        return Err(refused(
            "The query must be an object of names and values.",
            "Pass e.g. {\"page\": 2}.",
        ));
    };
    let mut ser = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in map {
        let v = match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => {
                return Err(refused(
                    format!("`{k}` must be a plain value."),
                    "Use strings, numbers or true/false in the query.",
                ))
            }
        };
        ser.append_pair(k, &v);
    }
    let s = ser.finish();
    Ok(if s.is_empty() { s } else { format!("?{s}") })
}

/// A REST connection for one call.
pub struct Rest<'a> {
    pub conn: &'a Connector,
    /// The key, opened.
    pub key: &'a str,
    pub http: &'a dyn Transport,
}

/// What came back, cleaned.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub asked: String,
    pub text: String,
    pub hidden: usize,
}

impl Rest<'_> {
    /// One GET of `path` (under an allowed prefix) with `query`.
    pub async fn get(&self, path: &str, query: Option<&Value>) -> Result<Answer, StepError> {
        let path = check_path(path)?;
        if !path_allowed(&path, &self.conn.paths) {
            return Err(refused(
                format!("{path} isn't under the paths the team allowed."),
                &format!("Read under: {}.", self.conn.paths.join(", ")),
            ));
        }
        let qs = query_string(query)?;
        let base = check_base(&self.conn.site)?;
        let header = check_header(&self.conn.header_name)?;
        let asked = format!("GET {path}{qs}");
        let reply = self
            .http
            .send(Request {
                method: Method::Get,
                url: format!("{base}{path}{qs}"),
                headers: vec![
                    (header, self.key.to_string()),
                    ("Accept".to_string(), "application/json".to_string()),
                ],
                body: None,
            })
            .await?;
        match reply.status {
            200..=299 => {}
            401 => {
                return Err(StepError::new(
                    "sign_in",
                    "The API didn't accept the key.",
                    "Enter the key again in Settings → Integrations.",
                ))
            }
            403 => {
                return Err(StepError::new(
                    "read",
                    "The key can't read that.",
                    "Read something the key has access to.",
                ))
            }
            404 => {
                return Err(StepError::new(
                    "read",
                    "The API has nothing at that path.",
                    "Check the path and the ids in it.",
                ))
            }
            s => {
                return Err(StepError::new(
                    "read",
                    format!("The API answered {s}."),
                    "Try again in a minute.",
                ))
            }
        }
        let (text, hidden) = match reply.json() {
            Some(mut v) => {
                let mut terms: Vec<String> = DEFAULT_HIDDEN.iter().map(|s| s.to_string()).collect();
                terms.extend(self.conn.hidden_fields.iter().cloned());
                let n = redact_all(&mut v, &hidden_terms(&terms));
                (serde_json::to_string_pretty(&v).unwrap_or_default(), n)
            }
            None => redact_text(&reply.body),
        };
        Ok(Answer {
            asked,
            text: clip(&text, MAX_CHARS),
            hidden,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::connectors::ConnKind;
    use crate::live::http::fake::Fake;
    use serde_json::json;

    fn conn() -> Connector {
        Connector {
            enabled: true,
            status: "ready".into(),
            site: "https://api.example.com".into(),
            header_name: "X-Api-Key".into(),
            paths: vec!["/v1/org/7/programs".into()],
            ..Connector::new(ConnKind::Rest)
        }
    }

    #[test]
    fn addresses_headers_and_paths_are_checked() {
        assert_eq!(
            check_base("https://API.example.com/").unwrap(),
            "https://api.example.com"
        );
        assert!(check_base("http://api.example.com").is_err());
        assert!(check_base("https://api.example.com/v1").is_err());
        assert!(check_base("https://1.2.3.4").is_err());
        assert!(check_header("X-Api-Key").is_ok());
        assert!(check_header("Cookie").is_err());
        assert!(check_header("X Api").is_err());
        assert!(check_path("/v1/org/7/programs/12").is_ok());
        for bad in [
            "v1/x",
            "/v1/../admin",
            "//evil.com/x",
            "/v1/x?y=1",
            "/v1/x#y",
            "/v1\\x",
        ] {
            assert!(check_path(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn only_paths_under_an_allowed_prefix() {
        let allowed = vec!["/v1/org/7/programs".to_string()];
        assert!(path_allowed("/v1/org/7/programs", &allowed));
        assert!(path_allowed("/v1/org/7/programs/12/sessions", &allowed));
        assert!(!path_allowed("/v1/org/7/programsx", &allowed));
        assert!(!path_allowed("/v1/org/8/programs", &allowed));
        assert!(!path_allowed("/v1/org/7/programs", &[]));
    }

    #[test]
    fn queries_are_plain_pairs() {
        assert_eq!(
            query_string(Some(&json!({"page": 2, "q": "a b"}))).unwrap(),
            "?page=2&q=a+b"
        );
        assert_eq!(query_string(None).unwrap(), "");
        assert!(query_string(Some(&json!({"x": {"y": 1}}))).is_err());
        assert!(query_string(Some(&json!([1]))).is_err());
    }

    #[tokio::test]
    async fn a_get_sends_the_key_in_its_header_and_hides_sensitive_fields() {
        let http = Fake::answering(vec![(
            200,
            r#"{"id":12,"name":"Pottery 1","email":"a@b.co","facility":"North"}"#,
        )]);
        let c = conn();
        let r = Rest {
            conn: &c,
            key: "k-123",
            http: &http,
        };
        let a = r
            .get(
                "/v1/org/7/programs/12",
                Some(&json!({"expand": "sessions"})),
            )
            .await
            .unwrap();
        let sent = http.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].method, Method::Get);
        assert_eq!(
            sent[0].url,
            "https://api.example.com/v1/org/7/programs/12?expand=sessions"
        );
        assert!(sent[0]
            .headers
            .iter()
            .any(|(k, v)| k == "X-Api-Key" && v == "k-123"));
        assert!(a.text.contains("\"facility\": \"North\""), "{}", a.text);
        assert!(!a.text.contains("a@b.co"));
        assert!(!a.text.contains("k-123"), "the key never comes back");
    }

    #[tokio::test]
    async fn a_path_outside_the_allowed_ones_is_never_requested() {
        let http = Fake::answering(vec![]);
        let c = conn();
        let r = Rest {
            conn: &c,
            key: "k",
            http: &http,
        };
        assert!(r.get("/v1/org/7/customers", None).await.is_err());
        assert!(r
            .get("/v1/org/7/programs/../customers", None)
            .await
            .is_err());
        assert!(http.sent().is_empty());
    }
}
