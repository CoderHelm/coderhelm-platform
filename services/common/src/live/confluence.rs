//! Confluence Cloud through its REST API with an Atlassian API token.
//!
//! Reads are GETs to the team's own `*.atlassian.net` site: a page by id, a
//! search, a page's children. The one write appends to the end of a page, and
//! only when the team turned writes on AND listed that page id; it reads the
//! page first and sends the next version, so it can never replace someone
//! else's newer edit. Nothing can create, move or delete a page.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};

use super::connectors::Connector;
use super::http::{Method, Request, Transport};
use super::redact::redact_text;
use super::StepError;

/// The most text one append may add.
pub const MAX_APPEND_CHARS: usize = 10_000;
const MAX_CQL_TEXT: usize = 200;

fn refused(message: impl Into<String>, fix: &str) -> StepError {
    StepError::new("read_only", message, fix)
}

/// Accepts only `https://<name>.atlassian.net` (no path), lowercased.
pub fn check_site(raw: &str) -> Result<String, StepError> {
    let fix = "Paste your Confluence address, like https://yourcompany.atlassian.net";
    let url = reqwest::Url::parse(raw.trim())
        .map_err(|_| StepError::new("reach", "That isn't a web address.", fix))?;
    let host = url.host_str().unwrap_or("").to_ascii_lowercase();
    let name = host.strip_suffix(".atlassian.net").unwrap_or("");
    let ok = url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.port().is_none()
        && !name.is_empty()
        && name.len() <= 63
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(format!("https://{host}"))
    } else {
        Err(StepError::new(
            "reach",
            "That isn't a Confluence Cloud address.",
            fix,
        ))
    }
}

/// A page id: digits only.
pub fn check_page_id(id: &str) -> Result<&str, StepError> {
    let id = id.trim();
    if !id.is_empty() && id.len() <= 20 && id.chars().all(|c| c.is_ascii_digit()) {
        Ok(id)
    } else {
        Err(refused(
            format!("{id} isn't a page id."),
            "Pass the number from the page's address, after /pages/.",
        ))
    }
}

/// A word or phrase placed inside double quotes in CQL.
fn cql_text(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// A space key: letters, digits, `~` (personal spaces) and `_`.
fn check_space(s: &str) -> Result<&str, StepError> {
    let s = s.trim();
    if !s.is_empty()
        && s.len() <= 255
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "~_".contains(c))
    {
        Ok(s)
    } else {
        Err(refused(
            format!("{s} isn't a space key."),
            "Pass a space key like ENG.",
        ))
    }
}

/// The search sent for `text`, limited to the team's spaces.
pub fn build_cql(
    text: &str,
    title_only: bool,
    space: Option<&str>,
    allowed: &[String],
) -> Result<String, StepError> {
    let text = text.trim();
    if text.is_empty() || text.len() > MAX_CQL_TEXT {
        return Err(refused(
            "Say what to search for (a few words).",
            "Pass `query`.",
        ));
    }
    let field = if title_only { "title" } else { "text" };
    let mut cql = format!("type = page AND {field} ~ \"{}\"", cql_text(text));
    match space.map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => {
            let s = check_space(s)?;
            if !allowed.is_empty() && !allowed.iter().any(|a| a.eq_ignore_ascii_case(s)) {
                return Err(refused(
                    format!("The team didn't share the {s} space."),
                    "Search one of the shared spaces, or leave `space` out.",
                ));
            }
            cql.push_str(&format!(" AND space = \"{s}\""));
        }
        None if !allowed.is_empty() => {
            let keys: Vec<String> = allowed
                .iter()
                .filter_map(|a| check_space(a).ok())
                .map(|a| format!("\"{a}\""))
                .collect();
            cql.push_str(&format!(" AND space IN ({})", keys.join(", ")));
        }
        None => {}
    }
    cql.push_str(" ORDER BY lastmodified DESC");
    Ok(cql)
}

/// Confluence's page markup as plain text a model can read: block tags
/// become line breaks, other tags are dropped, entities are decoded.
pub fn storage_to_text(html: &str) -> String {
    static BLOCK: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?i)</?(p|div|h[1-6]|li|tr|br|table|ul|ol|pre|blockquote)\b[^>]*>")
            .expect("valid")
    });
    static TAG: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"<[^>]*>").expect("valid"));
    static BLANKS: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"\n\s*\n+").expect("valid"));
    let t = BLOCK.replace_all(html, "\n");
    let t = TAG.replace_all(&t, "");
    let t = t
        .replace("&nbsp;", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&");
    BLANKS.replace_all(t.trim(), "\n\n").into_owned()
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Plain text (with `## ` headings and `- ` lists) as Confluence markup.
/// Everything is escaped, so the text can't add macros or scripts.
pub fn text_to_storage(text: &str) -> String {
    let mut out = String::new();
    for block in text.split("\n\n").map(str::trim).filter(|b| !b.is_empty()) {
        let lines: Vec<&str> = block.lines().map(str::trim).collect();
        if lines
            .iter()
            .all(|l| l.starts_with("- ") || l.starts_with("* "))
        {
            out.push_str("<ul>");
            for l in lines {
                out.push_str(&format!("<li>{}</li>", escape(&l[2..])));
            }
            out.push_str("</ul>");
        } else if let Some(h) = block
            .strip_prefix("## ")
            .or_else(|| block.strip_prefix("# "))
        {
            out.push_str(&format!("<h3>{}</h3>", escape(h)));
        } else {
            out.push_str(&format!(
                "<p>{}</p>",
                lines
                    .iter()
                    .map(|l| escape(l))
                    .collect::<Vec<_>>()
                    .join("<br/>")
            ));
        }
    }
    out
}

/// A Confluence connection for one call.
pub struct Confluence<'a> {
    pub conn: &'a Connector,
    /// The API token, opened.
    pub token: &'a str,
    pub http: &'a dyn Transport,
}

/// What came back, cleaned.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub asked: String,
    pub text: String,
    pub hidden: usize,
}

impl Confluence<'_> {
    fn auth(&self) -> String {
        format!(
            "Basic {}",
            STANDARD.encode(format!("{}:{}", self.conn.email, self.token))
        )
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, StepError> {
        let site = check_site(&self.conn.site)?;
        let reply = self
            .http
            .send(Request {
                method,
                url: format!("{site}{path}"),
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
                    "Confluence answered with something that isn't JSON.",
                    "Try again.",
                )
            }),
            401 => Err(StepError::new(
                "sign_in",
                "Confluence didn't accept the email and token.",
                "Paste a new API token in Data sources.",
            )),
            403 => Err(StepError::new(
                "read",
                "That Confluence account can't see this.",
                "Give the account access to the space, or use one it can see.",
            )),
            404 => Err(StepError::new(
                "read",
                "Confluence has no page with that id (or the account can't see it).",
                "Check the page id.",
            )),
            409 => Err(StepError::new(
                "read",
                "Someone edited the page at the same moment.",
                "Read it again and retry once.",
            )),
            s => Err(StepError::new(
                "read",
                format!("Confluence answered {s}."),
                "Try again in a minute.",
            )),
        }
    }

    /// The space key of a space id, to check it against the team's list.
    async fn space_key(&self, space_id: &str) -> Result<String, StepError> {
        if !space_id.chars().all(|c| c.is_ascii_digit()) || space_id.is_empty() {
            return Ok(String::new());
        }
        let v = self
            .call(
                Method::Get,
                &format!("/wiki/api/v2/spaces/{space_id}"),
                None,
            )
            .await?;
        Ok(v["key"].as_str().unwrap_or("").to_string())
    }

    async fn page_raw(&self, id: &str) -> Result<Value, StepError> {
        let id = check_page_id(id)?;
        let page = self
            .call(
                Method::Get,
                &format!("/wiki/api/v2/pages/{id}?body-format=storage"),
                None,
            )
            .await?;
        if !self.conn.spaces.is_empty() {
            let key = self
                .space_key(page["spaceId"].as_str().unwrap_or(""))
                .await?;
            if !self
                .conn
                .spaces
                .iter()
                .any(|s| s.eq_ignore_ascii_case(&key))
            {
                return Err(refused(
                    "That page is in a space the team didn't share.",
                    "Read pages in the shared spaces only.",
                ));
            }
        }
        Ok(page)
    }

    /// One page as text.
    pub async fn page(&self, id: &str) -> Result<Answer, StepError> {
        let page = self.page_raw(id).await?;
        let body = storage_to_text(page["body"]["storage"]["value"].as_str().unwrap_or(""));
        let text = format!(
            "# {}\n(page {}, version {})\n\n{}",
            page["title"].as_str().unwrap_or(""),
            page["id"].as_str().unwrap_or(id),
            page["version"]["number"],
            body
        );
        let (text, hidden) = redact_text(&text);
        Ok(Answer {
            asked: format!("read page {id}"),
            text,
            hidden,
        })
    }

    /// Pages matching words (in the text or only the title).
    pub async fn search(
        &self,
        query: &str,
        title_only: bool,
        space: Option<&str>,
    ) -> Result<Answer, StepError> {
        let cql = build_cql(query, title_only, space, &self.conn.spaces)?;
        let path = format!(
            "/wiki/rest/api/search?limit=15&cql={}",
            url::form_urlencoded::byte_serialize(cql.as_bytes()).collect::<String>()
        );
        let v = self.call(Method::Get, &path, None).await?;
        let mut lines = Vec::new();
        for r in v["results"].as_array().cloned().unwrap_or_default() {
            let c = &r["content"];
            lines.push(format!(
                "- {} (page {}, space {}, changed {}): {}",
                c["title"].as_str().or(r["title"].as_str()).unwrap_or(""),
                c["id"].as_str().unwrap_or(""),
                r["resultGlobalContainer"]["title"].as_str().unwrap_or(""),
                r["friendlyLastModified"].as_str().unwrap_or(""),
                storage_to_text(r["excerpt"].as_str().unwrap_or("")).replace('\n', " ")
            ));
        }
        let text = if lines.is_empty() {
            "No pages matched.".to_string()
        } else {
            lines.join("\n")
        };
        let (text, hidden) = redact_text(&text);
        Ok(Answer {
            asked: cql,
            text,
            hidden,
        })
    }

    /// Adds `text` to the end of page `id`. Refused unless the team turned
    /// writes on and listed this page. Returns the new version number.
    pub async fn append(&self, id: &str, text: &str) -> Result<Answer, StepError> {
        let id = check_page_id(id)?;
        if !self.conn.may_write_page(id) {
            return Err(refused(
                format!("Coderhelm may not write to page {id}."),
                "An admin can allow it in Data sources → Confluence (writes on, and the page on the list). Put the text in your reply for a person to add.",
            ));
        }
        let text = text.trim();
        if text.is_empty() || text.len() > MAX_APPEND_CHARS {
            return Err(refused(
                format!("Appends are 1 to {MAX_APPEND_CHARS} characters."),
                "Shorten the text.",
            ));
        }
        let (clean, hidden) = redact_text(text);
        let page = self.page_raw(id).await?;
        let version = page["version"]["number"].as_i64().unwrap_or(0);
        let old = page["body"]["storage"]["value"].as_str().unwrap_or("");
        let body = json!({
            "id": id,
            "status": "current",
            "title": page["title"],
            "body": { "representation": "storage", "value": format!("{old}{}", text_to_storage(&clean)) },
            "version": { "number": version + 1, "message": "Added by Coderhelm" }
        });
        self.call(Method::Put, &format!("/wiki/api/v2/pages/{id}"), Some(body))
            .await?;
        Ok(Answer {
            asked: format!("append {} characters to page {id}", clean.len()),
            text: format!("Added to page {id} (now version {}).", version + 1),
            hidden,
        })
    }

    /// Checks the email and token: who they sign in as.
    pub async fn whoami(&self) -> Result<String, StepError> {
        let v = self
            .call(Method::Get, "/wiki/rest/api/user/current", None)
            .await?;
        Ok(v["displayName"]
            .as_str()
            .or(v["publicName"].as_str())
            .unwrap_or("the account")
            .to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::connectors::ConnKind;
    use crate::live::http::fake::Fake;

    fn conn() -> Connector {
        Connector {
            enabled: true,
            status: "ready".into(),
            site: "https://example.atlassian.net".into(),
            email: "bot@example.com".into(),
            write_enabled: true,
            write_pages: vec!["42".into()],
            ..Connector::new(ConnKind::Confluence)
        }
    }

    const PAGE: &str = r#"{"id":"42","title":"Lessons","spaceId":"7","version":{"number":3},"body":{"storage":{"value":"<h1>Lessons</h1><p>one &amp; two</p>"}}}"#;

    #[test]
    fn only_atlassian_sites_are_accepted() {
        assert_eq!(
            check_site("https://Example.atlassian.net/wiki").unwrap(),
            "https://example.atlassian.net"
        );
        assert!(check_site("http://example.atlassian.net").is_err());
        assert!(check_site("https://example.com").is_err());
        assert!(check_site("https://evil.com/.atlassian.net").is_err());
        assert!(check_site("https://u:p@example.atlassian.net").is_err());
        assert!(check_site("https://example.atlassian.net:8443").is_err());
    }

    #[test]
    fn searches_stay_in_shared_spaces_and_quotes_cannot_escape() {
        let shared = vec!["ENG".to_string()];
        let q = build_cql("deploy \"x", false, None, &shared).unwrap();
        assert!(q.contains("text ~ \"deploy \\\"x\""), "{q}");
        assert!(q.contains("space IN (\"ENG\")"), "{q}");
        assert!(build_cql("x", false, Some("HR"), &shared).is_err());
        assert!(build_cql("x", true, Some("eng"), &shared)
            .unwrap()
            .contains("title ~"));
        assert!(build_cql("", false, None, &[]).is_err());
        assert!(build_cql("x", false, Some("bad space"), &[]).is_err());
    }

    #[test]
    fn markup_reads_as_text_and_text_is_escaped() {
        assert_eq!(
            storage_to_text("<h1>T</h1><p>a &amp; b</p><ul><li>x</li></ul>"),
            "T\n\na & b\n\nx"
        );
        let s = text_to_storage("## When\n\nSaw <script> & more\n\n- one\n- two");
        assert_eq!(
            s,
            "<h3>When</h3><p>Saw &lt;script&gt; &amp; more</p><ul><li>one</li><li>two</li></ul>"
        );
    }

    #[tokio::test]
    async fn reading_a_page_is_one_get() {
        let http = Fake::answering(vec![(200, PAGE)]);
        let c = conn();
        let a = Confluence {
            conn: &c,
            token: "t",
            http: &http,
        }
        .page("42")
        .await
        .unwrap();
        assert!(a.text.contains("# Lessons"));
        assert!(a.text.contains("one & two"));
        let sent = http.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].method, Method::Get);
        assert_eq!(
            sent[0].url,
            "https://example.atlassian.net/wiki/api/v2/pages/42?body-format=storage"
        );
        let auth = &sent[0]
            .headers
            .iter()
            .find(|(k, _)| k == "Authorization")
            .unwrap()
            .1;
        assert_eq!(
            auth,
            &format!("Basic {}", STANDARD.encode("bot@example.com:t"))
        );
    }

    #[tokio::test]
    async fn a_page_outside_the_shared_spaces_is_refused() {
        let http = Fake::answering(vec![(200, PAGE), (200, r#"{"key":"HR"}"#)]);
        let mut c = conn();
        c.spaces = vec!["ENG".into()];
        let e = Confluence {
            conn: &c,
            token: "t",
            http: &http,
        }
        .page("42")
        .await
        .unwrap_err();
        assert!(e.is_refusal());
    }

    #[tokio::test]
    async fn an_append_reads_then_writes_the_next_version() {
        let http = Fake::answering(vec![(200, PAGE), (200, r#"{"id":"42"}"#)]);
        let c = conn();
        let a = Confluence {
            conn: &c,
            token: "t",
            http: &http,
        }
        .append(
            "42",
            "## New lesson\n\nCheck the queue first. Mail ops@example.com",
        )
        .await
        .unwrap();
        assert!(a.text.contains("version 4"));
        let sent = http.sent();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].method, Method::Get);
        assert_eq!(sent[1].method, Method::Put);
        let body = sent[1].body.as_ref().unwrap();
        assert_eq!(body["version"]["number"], 4);
        let value = body["body"]["value"].as_str().unwrap();
        assert!(
            value.starts_with("<h1>Lessons</h1><p>one &amp; two</p>"),
            "keeps the page: {value}"
        );
        assert!(
            value.contains("<h3>New lesson</h3><p>Check the queue first."),
            "{value}"
        );
        assert!(!value.contains("ops@example.com"), "redacted");
    }

    #[tokio::test]
    async fn appends_outside_the_list_or_with_writes_off_are_never_sent() {
        let http = Fake::default();
        let c = conn();
        let cf = Confluence {
            conn: &c,
            token: "t",
            http: &http,
        };
        assert!(cf.append("43", "x").await.unwrap_err().is_refusal());
        let mut off = conn();
        off.write_enabled = false;
        let cf = Confluence {
            conn: &off,
            token: "t",
            http: &http,
        };
        assert!(cf.append("42", "x").await.unwrap_err().is_refusal());
        let c = conn();
        let cf = Confluence {
            conn: &c,
            token: "t",
            http: &http,
        };
        assert!(cf.append("42", "").await.unwrap_err().is_refusal());
        assert!(cf
            .append("42", &"x".repeat(MAX_APPEND_CHARS + 1))
            .await
            .unwrap_err()
            .is_refusal());
        assert!(http.sent().is_empty());
    }

    #[tokio::test]
    async fn a_search_is_one_get_with_the_checked_cql() {
        let http = Fake::answering(vec![(
            200,
            r#"{"results":[{"content":{"id":"42","title":"Lessons"},"resultGlobalContainer":{"title":"Eng"},"excerpt":"a <b>b</b>","friendlyLastModified":"yesterday"}]}"#,
        )]);
        let c = conn();
        let a = Confluence {
            conn: &c,
            token: "t",
            http: &http,
        }
        .search("lessons", true, None)
        .await
        .unwrap();
        assert!(a.text.contains("Lessons (page 42"), "{}", a.text);
        let sent = http.sent();
        assert_eq!(sent[0].method, Method::Get);
        assert!(sent[0]
            .url
            .contains("/wiki/rest/api/search?limit=15&cql=type+%3D+page"));
    }
}
