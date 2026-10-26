//! Connecting a source with a token pasted in the Knowledge add flow, for
//! installs without the provider's OAuth app (and for teams that prefer a
//! token).
//!
//! The token is checked against the provider before anything is saved, so
//! a token the provider refuses is never stored, and the person sees one
//! sentence that says what to fix. Saving goes through the same code the
//! Integrations page uses (the gateway's `plugins` and `connectors`), so a
//! token saved here shows there and the other way round.
//!
//! | kind | what is pasted | where it is saved |
//! |---|---|---|
//! | Notion | an internal connection's API token | the Notion entry on Integrations (`PLUGIN#notion`, sealed) |
//! | Confluence | site, email and API token | the Confluence connection on Integrations (sealed) |
//! | Google Drive | nothing: Drive connects only through Google sign-in | |

use super::connector::ConnectorError;
use super::notion::Notion;
use crate::mcp_remote::http::Http;

/// Longest token accepted (Notion's are about 50 characters).
const MAX_TOKEN: usize = 400;

/// What went wrong, as one sentence that says the fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenError(pub String);

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A pasted token, trimmed, or why it can't be one.
pub fn clean_token(raw: &str, label: &str) -> Result<String, TokenError> {
    let t = raw.trim();
    if t.is_empty() {
        return Err(TokenError(format!("Paste the {label} token first.")));
    }
    if t.len() > MAX_TOKEN || t.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(TokenError(format!(
            "That doesn't look like a {label} token: paste only the token, as one line."
        )));
    }
    Ok(t.to_string())
}

/// The Notion `Authorization` header for a token.
pub fn notion_auth(token: &str) -> String {
    format!("Bearer {}", token.trim())
}

/// What a Notion token can see: how many pages and databases are shared
/// with it (up to 100; the first search page is enough to know).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotionCheck {
    pub shared: usize,
}

/// The sentence shown when the token works but nothing is shared with it.
pub const NOTION_NOTHING_SHARED: &str = "The token works, but no pages are shared with it yet: in Notion, open a page, choose ••• at the top right, then Connections, then + Add connection, and pick your connection.";

/// Checks a Notion token with one search call.
pub async fn check_notion(http: &dyn Http, token: &str) -> Result<NotionCheck, TokenError> {
    let n = Notion {
        http,
        auth: notion_auth(token),
    };
    match n.search("", 1).await {
        Ok((entries, _)) => Ok(NotionCheck {
            shared: entries.len(),
        }),
        Err(e) => Err(notion_error(&e)),
    }
}

/// One sentence with the fix for a failed Notion check.
pub fn notion_error(e: &ConnectorError) -> TokenError {
    TokenError(match e {
        ConnectorError::Reconnect(_) | ConnectorError::Config(_) => {
            "Notion didn't accept that token: copy the API token again from the Configuration tab of your Notion connection.".into()
        }
        ConnectorError::Forbidden(_) => {
            "Notion says the token can't read content: tick Read content under Capabilities on the Configuration tab, save, and try again.".into()
        }
        ConnectorError::RateLimited(_) => {
            "Notion asked us to slow down: try again in a minute.".into()
        }
        ConnectorError::NotFound(_) | ConnectorError::Temporary(_) => {
            "Notion didn't answer just now: try again in a minute.".into()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::sources::notion::API;
    use crate::mcp_remote::http::fake::{json as fjson, Fake};
    use crate::mcp_remote::http::Method;
    use serde_json::json;

    #[test]
    fn a_pasted_token_is_trimmed_and_must_be_one_line() {
        assert_eq!(
            clean_token("  ntn_abc123  \n", "Notion").unwrap(),
            "ntn_abc123"
        );
        assert_eq!(
            clean_token("   ", "Notion").unwrap_err().0,
            "Paste the Notion token first."
        );
        assert!(clean_token("ntn_abc 123", "Notion").is_err());
        assert!(clean_token("Authorization: Bearer x", "Notion").is_err());
        assert!(clean_token(&"a".repeat(MAX_TOKEN + 1), "Notion").is_err());
        // Older tokens start with secret_; Notion decides, not a prefix rule.
        assert!(clean_token("secret_abc", "Notion").is_ok());
    }

    #[tokio::test]
    async fn a_working_token_counts_what_is_shared_with_it() {
        let http = Fake::default();
        http.on(
            Method::Post,
            &format!("{API}/search"),
            fjson(
                200,
                json!({"results": [
                    {"object": "page", "id": "a1", "parent": {"type": "workspace"}, "properties": {}},
                    {"object": "database", "id": "b2", "parent": {"type": "workspace"}, "title": []}
                ], "has_more": true, "next_cursor": "c"}),
            ),
        );
        let c = check_notion(&http, "ntn_good").await.unwrap();
        assert_eq!(c.shared, 2);
        let sent = http.sent_to(&format!("{API}/search"));
        assert_eq!(sent.len(), 1, "one call is enough");
        assert_eq!(
            sent[0].header_value("Authorization"),
            Some("Bearer ntn_good")
        );
        assert!(sent[0].header_value("Notion-Version").is_some());
    }

    #[tokio::test]
    async fn a_token_with_nothing_shared_works_but_sees_nothing() {
        let http = Fake::default();
        http.on(
            Method::Post,
            &format!("{API}/search"),
            fjson(200, json!({"results": [], "has_more": false})),
        );
        assert_eq!(check_notion(&http, "ntn_x").await.unwrap().shared, 0);
        assert!(NOTION_NOTHING_SHARED.contains("Connections"));
    }

    #[tokio::test]
    async fn a_refused_token_says_where_to_copy_it_again() {
        let http = Fake::default();
        http.on(
            Method::Post,
            &format!("{API}/search"),
            fjson(
                401,
                json!({"object": "error", "code": "unauthorized", "message": "API token is invalid."}),
            ),
        );
        let e = check_notion(&http, "ntn_bad").await.unwrap_err();
        assert!(e.0.starts_with("Notion didn't accept that token"), "{e}");
        assert!(e.0.contains("Configuration tab"));
    }

    #[test]
    fn every_failure_is_one_sentence_with_a_fix() {
        for e in [
            ConnectorError::Reconnect("x".into()),
            ConnectorError::Config("x".into()),
            ConnectorError::Forbidden("x".into()),
            ConnectorError::RateLimited("x".into()),
            ConnectorError::NotFound("x".into()),
            ConnectorError::Temporary("x".into()),
        ] {
            let s = notion_error(&e).0;
            assert!(s.ends_with('.'), "{s}");
            assert_eq!(s.matches(". ").count(), 0, "one sentence: {s}");
            assert!(s.contains(':'), "says the fix: {s}");
        }
    }
}
