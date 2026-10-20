//! A team's OpenSearch (or Elasticsearch) cluster, read only, signed in with
//! a user and password (HTTP basic auth).
//!
//! Agents can list the indexes, read one index's field mapping, count the
//! documents a query matches, and search with a query. Every request is built
//! here: an index name is checked before it goes into a path, a search body
//! may hold only the read parts of the query language (no scripts), and
//! nothing else is sent. When the team lists indexes, only those (and
//! patterns matching them) can be read. Results go through the same
//! redaction as every other source: hidden fields by name (emails, phones,
//! addresses and the like, plus any the team adds), and emails, card numbers
//! and keys wherever they appear in text.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Map, Value};

use super::connectors::Connector;
use super::http::{Method, Request, Transport};
use super::redact::{hidden_terms, redact_all};
use super::StepError;

/// Documents one search returns.
pub const MAX_HITS: u64 = 20;
/// The furthest a search may page in.
pub const MAX_FROM: u64 = 200;
/// Fields listed from one mapping.
const MAX_FIELDS: usize = 400;
/// Indexes listed.
const MAX_INDEXES: usize = 200;
/// The keys a search body may hold at its top level.
const BODY_KEYS: &[&str] = &[
    "query",
    "size",
    "from",
    "sort",
    "_source",
    "aggs",
    "aggregations",
    "track_total_hits",
];

fn refused(message: impl Into<String>, fix: &str) -> StepError {
    StepError::new("read_only", message, fix)
}

/// Accepts `https://host` or `https://host:port`, no path, no credentials.
pub fn check_endpoint(raw: &str) -> Result<String, StepError> {
    let fix = "Paste the cluster's address, like https://search.example.com or https://search.example.com:9200";
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
            "That isn't an https address of a search cluster.",
            fix,
        ));
    }
    Ok(match url.port() {
        Some(p) => format!("https://{host}:{p}"),
        None => format!("https://{host}"),
    })
}

/// An index name or pattern as it may go into a path: lowercase letters,
/// digits, `-`, `_`, `.`, `*`, and `,` between several. A leading `_` or
/// `-` is refused, so nothing can reach an API path or exclude.
pub fn check_index(raw: &str) -> Result<String, StepError> {
    let s = raw.trim();
    let ok = !s.is_empty()
        && s.len() <= 255
        && s.split(',').all(|part| {
            !part.is_empty()
                && !part.starts_with('_')
                && !part.starts_with('-')
                && part
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "-_.*".contains(c))
        });
    if ok {
        Ok(s.to_string())
    } else {
        Err(refused(
            format!("{s} isn't an index name."),
            "Pass an index name (lowercase), a pattern like logs-*, or several separated by commas.",
        ))
    }
}

/// Whether `name` matches `pattern` (`*` matches any run of characters).
fn glob(pattern: &str, name: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == name;
    }
    let mut rest = name;
    for (i, part) in parts.iter().enumerate() {
        if i == 0 {
            match rest.strip_prefix(part) {
                Some(r) => rest = r,
                None => return false,
            }
        } else if i == parts.len() - 1 {
            return part.is_empty() || rest.ends_with(part);
        } else if let Some(at) = rest.find(part) {
            rest = &rest[at + part.len()..];
        } else {
            return false;
        }
    }
    true
}

/// Whether the team's list lets `index` be read. An empty list allows any.
/// A pattern asked for must itself sit inside a listed pattern: every name
/// it could match must be allowed.
pub fn index_allowed(index: &str, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    index.split(',').all(|part| {
        allowed.iter().any(|a| {
            if part.contains('*') {
                // `orders-*` inside `orders-*` or `*`; never wider than listed.
                a == "*"
                    || a == part
                    || (a.ends_with('*') && part.starts_with(a.trim_end_matches('*')))
            } else {
                glob(a, part)
            }
        })
    })
}

/// Refuses anything in a search body that could run code or isn't a read:
/// only the listed top-level keys, and no `script` anywhere inside.
pub fn check_body(body: &Value) -> Result<Map<String, Value>, StepError> {
    let Some(map) = body.as_object() else {
        return Err(refused(
            "The search body must be a JSON object.",
            "Pass e.g. {\"query\": {\"term\": {\"id\": \"…\"}}}.",
        ));
    };
    if let Some(k) = map.keys().find(|k| !BODY_KEYS.contains(&k.as_str())) {
        return Err(refused(
            format!("`{k}` isn't allowed in a search."),
            "Use query, size, from, sort, _source and aggs only.",
        ));
    }
    fn has_script(v: &Value) -> bool {
        match v {
            Value::Object(m) => m.iter().any(|(k, v)| k.contains("script") || has_script(v)),
            Value::Array(a) => a.iter().any(has_script),
            _ => false,
        }
    }
    if has_script(body) {
        return Err(refused(
            "Scripts aren't allowed in a search.",
            "Use plain queries and aggregations.",
        ));
    }
    let mut out = map.clone();
    let size = out
        .get("size")
        .and_then(Value::as_u64)
        .unwrap_or(10)
        .min(MAX_HITS);
    out.insert("size".into(), json!(size));
    if let Some(from) = out.get("from").and_then(Value::as_u64) {
        out.insert("from".into(), json!(from.min(MAX_FROM)));
    }
    Ok(out)
}

/// A cluster connection for one call.
pub struct OpenSearch<'a> {
    pub conn: &'a Connector,
    /// The password, opened.
    pub password: &'a str,
    pub http: &'a dyn Transport,
}

/// What came back, cleaned.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub asked: String,
    pub text: String,
    pub hidden: usize,
}

impl OpenSearch<'_> {
    fn auth(&self) -> String {
        format!(
            "Basic {}",
            STANDARD.encode(format!("{}:{}", self.conn.email, self.password))
        )
    }

    fn terms(&self) -> Vec<String> {
        let mut hidden: Vec<String> = super::redact::DEFAULT_HIDDEN
            .iter()
            .map(|s| s.to_string())
            .collect();
        hidden.extend(self.conn.hidden_fields.iter().cloned());
        hidden_terms(&hidden)
    }

    fn allowed(&self, index: &str) -> Result<String, StepError> {
        let index = check_index(index)?;
        if !index_allowed(&index, &self.conn.indexes) {
            return Err(refused(
                format!("The team didn't share {index}."),
                "Read one of the shared indexes (list them with opensearch_indexes).",
            ));
        }
        Ok(index)
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, StepError> {
        let base = check_endpoint(&self.conn.site)?;
        let reply = self
            .http
            .send(Request {
                method,
                url: format!("{base}{path}"),
                headers: vec![
                    ("Authorization".to_string(), self.auth()),
                    ("Accept".to_string(), "application/json".to_string()),
                ],
                body,
            })
            .await?;
        match reply.status {
            200..=299 => reply.json().ok_or_else(|| {
                StepError::new(
                    "read",
                    "The cluster answered with something that isn't JSON.",
                    "Check the address points at the cluster itself.",
                )
            }),
            401 => Err(StepError::new(
                "sign_in",
                "The cluster didn't accept the user and password.",
                "Enter them again in Settings → Integrations.",
            )),
            403 => Err(StepError::new(
                "read",
                "That user can't read this.",
                "Give the user read access to the index, or read one it can see.",
            )),
            404 => Err(StepError::new(
                "read",
                "No such index (or the user can't see it).",
                "List the indexes with opensearch_indexes.",
            )),
            400 => {
                let why = reply
                    .json()
                    .and_then(|v| {
                        v["error"]["root_cause"][0]["reason"]
                            .as_str()
                            .or(v["error"]["reason"].as_str())
                            .map(str::to_string)
                    })
                    .unwrap_or_default();
                Err(StepError::new(
                    "read",
                    format!(
                        "The cluster refused the query: {}",
                        super::connectors::clip(&why, 300)
                    ),
                    "Fix the query and try again.",
                ))
            }
            s => Err(StepError::new(
                "read",
                format!("The cluster answered {s}."),
                "Try again in a minute.",
            )),
        }
    }

    fn clean(&self, mut v: Value) -> (String, usize) {
        let hidden = redact_all(&mut v, &self.terms());
        (serde_json::to_string_pretty(&v).unwrap_or_default(), hidden)
    }

    /// The indexes the team shared, with document counts.
    pub async fn indexes(&self) -> Result<Answer, StepError> {
        let v = self
            .call(
                Method::Get,
                "/_cat/indices?format=json&h=index,docs.count,store.size&s=index",
                None,
            )
            .await?;
        let mut lines = Vec::new();
        for i in v.as_array().cloned().unwrap_or_default() {
            let name = i["index"].as_str().unwrap_or("");
            if name.starts_with('.') || !index_allowed(name, &self.conn.indexes) {
                continue;
            }
            lines.push(format!(
                "- {name} ({} documents, {})",
                i["docs.count"].as_str().unwrap_or("?"),
                i["store.size"].as_str().unwrap_or("?")
            ));
            if lines.len() == MAX_INDEXES {
                lines.push("… more not shown".into());
                break;
            }
        }
        Ok(Answer {
            asked: "list indexes".into(),
            text: if lines.is_empty() {
                "No shared indexes.".into()
            } else {
                lines.join("\n")
            },
            hidden: 0,
        })
    }

    /// One index's fields and their types.
    pub async fn mapping(&self, index: &str) -> Result<Answer, StepError> {
        let index = self.allowed(index)?;
        let v = self
            .call(Method::Get, &format!("/{index}/_mapping"), None)
            .await?;
        let mut lines = Vec::new();
        fn walk(prefix: &str, props: &Value, out: &mut Vec<String>) {
            if let Some(m) = props.as_object() {
                for (k, f) in m {
                    let path = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    match f["properties"].as_object() {
                        Some(_) => walk(&path, &f["properties"], out),
                        None => out.push(format!(
                            "{path}: {}",
                            f["type"].as_str().unwrap_or("object")
                        )),
                    }
                }
            }
        }
        for (name, m) in v.as_object().cloned().unwrap_or_default() {
            lines.push(format!("index {name}"));
            let mut fields = Vec::new();
            walk("", &m["mappings"]["properties"], &mut fields);
            fields.truncate(MAX_FIELDS);
            lines.extend(fields.into_iter().map(|f| format!("  {f}")));
        }
        Ok(Answer {
            asked: format!("mapping of {index}"),
            text: lines.join("\n"),
            hidden: 0,
        })
    }

    /// Documents matching a query (at most [`MAX_HITS`]), and the total.
    pub async fn search(&self, index: &str, body: &Value) -> Result<Answer, StepError> {
        let index = self.allowed(index)?;
        let body = Value::Object(check_body(body)?);
        let asked = format!(
            "search {index}: {}",
            super::connectors::clip(&body.to_string(), 400)
        );
        let v = self
            .call(Method::Post, &format!("/{index}/_search"), Some(body))
            .await?;
        let keep = json!({
            "total": v["hits"]["total"],
            "hits": v["hits"]["hits"]
                .as_array()
                .map(|a| a.iter().map(|h| json!({
                    "_index": h["_index"],
                    "_id": h["_id"],
                    "_source": h["_source"],
                })).collect::<Vec<_>>())
                .unwrap_or_default(),
            "aggregations": v.get("aggregations").cloned().unwrap_or(Value::Null),
        });
        let (text, hidden) = self.clean(keep);
        Ok(Answer {
            asked,
            text,
            hidden,
        })
    }

    /// How many documents a query matches.
    pub async fn count(&self, index: &str, query: Option<&Value>) -> Result<Answer, StepError> {
        let index = self.allowed(index)?;
        let body = match query {
            Some(q) => {
                let mut b = check_body(&json!({ "query": q }))?;
                b.remove("size");
                Some(Value::Object(b))
            }
            None => None,
        };
        let asked = format!(
            "count {index}{}",
            body.as_ref()
                .map(|b| format!(": {}", super::connectors::clip(&b.to_string(), 400)))
                .unwrap_or_default()
        );
        let method = if body.is_some() {
            Method::Post
        } else {
            Method::Get
        };
        let v = self.call(method, &format!("/{index}/_count"), body).await?;
        Ok(Answer {
            asked,
            text: format!("{} documents", v["count"]),
            hidden: 0,
        })
    }

    /// Checks the user and password: the cluster's name and version.
    pub async fn whoami(&self) -> Result<String, StepError> {
        let v = self.call(Method::Get, "/", None).await?;
        Ok(format!(
            "Signed in; cluster {} ({} {})",
            v["cluster_name"].as_str().unwrap_or("?"),
            v["version"]["distribution"]
                .as_str()
                .unwrap_or("elasticsearch"),
            v["version"]["number"].as_str().unwrap_or("?")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::connectors::ConnKind;
    use crate::live::http::fake::Fake;

    fn conn(indexes: &[&str]) -> Connector {
        Connector {
            enabled: true,
            status: "ready".into(),
            site: "https://search.example.com".into(),
            email: "reader".into(),
            indexes: indexes.iter().map(|s| s.to_string()).collect(),
            ..Connector::new(ConnKind::OpenSearch)
        }
    }

    #[test]
    fn endpoints_are_https_hosts_only() {
        assert_eq!(
            check_endpoint("https://Search.Example.com/").unwrap(),
            "https://search.example.com"
        );
        assert_eq!(
            check_endpoint("https://search.example.com:9200").unwrap(),
            "https://search.example.com:9200"
        );
        for bad in [
            "http://search.example.com",
            "https://user:pw@search.example.com",
            "https://search.example.com/orders",
            "https://10.0.0.1",
            "not a url",
        ] {
            assert!(check_endpoint(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn index_names_cannot_reach_api_paths() {
        assert!(check_index("orders").is_ok());
        assert!(check_index("orders-*,customers").is_ok());
        for bad in [
            "_cat",
            "_all",
            "-orders",
            "Orders",
            "orders/_doc",
            "a b",
            "",
        ] {
            assert!(check_index(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn only_shared_indexes_can_be_read() {
        let shared = vec!["orders".to_string(), "logs-*".to_string()];
        assert!(index_allowed("orders", &shared));
        assert!(index_allowed("logs-2026", &shared));
        assert!(index_allowed("logs-*", &shared));
        assert!(!index_allowed("customers", &shared));
        assert!(
            !index_allowed("*", &shared),
            "a pattern can't be wider than listed"
        );
        assert!(!index_allowed("orders,customers", &shared));
        assert!(index_allowed("anything", &[]));
    }

    #[test]
    fn a_search_body_holds_only_reads_and_is_capped() {
        let b = check_body(&json!({"query": {"term": {"id": "x"}}, "size": 500, "from": 9000}))
            .unwrap();
        assert_eq!(b["size"], json!(MAX_HITS));
        assert_eq!(b["from"], json!(MAX_FROM));
        assert!(check_body(&json!({"script": {}})).is_err());
        assert!(check_body(&json!({"query": {"script": {"script": "1"}}})).is_err());
        assert!(
            check_body(&json!({"query": {"bool": {"filter": [{"script_score": {}}]}}})).is_err()
        );
        assert!(check_body(&json!({"query": {}, "update": {}})).is_err());
        assert!(check_body(&json!("x")).is_err());
    }

    #[tokio::test]
    async fn a_search_posts_to_the_index_and_hides_sensitive_fields() {
        let http = Fake::answering(vec![(
            200,
            r#"{"hits":{"total":{"value":1},"hits":[{"_index":"orders","_id":"1","_source":{"id":"1","status":"open","email":"a@b.co","phone":"555-123-4567","note":"ask c@d.co"}}]}}"#,
        )]);
        let c = conn(&["orders"]);
        let os = OpenSearch {
            conn: &c,
            password: "pw",
            http: &http,
        };
        let a = os
            .search("orders", &json!({"query": {"term": {"id": "1"}}}))
            .await
            .unwrap();
        let sent = http.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].method, Method::Post);
        assert_eq!(sent[0].url, "https://search.example.com/orders/_search");
        assert!(a.text.contains("\"status\": \"open\""), "{}", a.text);
        assert!(!a.text.contains("a@b.co"), "{}", a.text);
        assert!(!a.text.contains("555-123-4567"), "{}", a.text);
        assert!(!a.text.contains("c@d.co"), "email in free text: {}", a.text);
        assert!(a.hidden >= 3);
    }

    #[tokio::test]
    async fn an_index_the_team_didnt_share_is_never_requested() {
        let http = Fake::answering(vec![]);
        let c = conn(&["orders"]);
        let os = OpenSearch {
            conn: &c,
            password: "pw",
            http: &http,
        };
        assert!(os.search("customers", &json!({"query": {}})).await.is_err());
        assert!(os.mapping("_security").await.is_err());
        assert!(http.sent().is_empty());
    }

    #[tokio::test]
    async fn the_index_list_shows_only_shared_indexes() {
        let http = Fake::answering(vec![(
            200,
            r#"[{"index":"orders","docs.count":"10","store.size":"1kb"},{"index":"customers","docs.count":"5","store.size":"1kb"},{"index":".kibana","docs.count":"1","store.size":"1kb"}]"#,
        )]);
        let c = conn(&["orders"]);
        let os = OpenSearch {
            conn: &c,
            password: "pw",
            http: &http,
        };
        let a = os.indexes().await.unwrap();
        assert!(a.text.contains("orders (10 documents"), "{}", a.text);
        assert!(!a.text.contains("customers"));
        assert!(!a.text.contains("kibana"));
    }
}
