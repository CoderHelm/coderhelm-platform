//! Release notes into a Confluence changelog, through the Confluence part of
//! the team's Atlassian connection (its email and API token), not through the
//! Jira app.
//!
//! The Jira app used to do this with its own Confluence scopes, which made
//! every Jira app release one an Atlassian site admin had to accept, across
//! two Atlassian products. The Jira app is Jira-only now; this does the same
//! steps from CoderHelm: under the parent page, a container page
//! ("Changelog"), under it one page per year, and the release's entry
//! upserted at the top of the year page (found again by its anchor, so a
//! regenerated entry replaces itself).

use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};

use super::confluence::{check_page_id, check_site};
use super::http::{Method, Request, Transport};

/// Where and what to publish.
#[derive(Debug, Clone)]
pub struct Entry<'a> {
    pub parent_id: &'a str,
    /// The container page's title; empty is "Changelog".
    pub container: &'a str,
    pub year: &'a str,
    /// `owner/repo@tag`: the anchor that finds this entry again.
    pub key: &'a str,
    /// The entry, in Confluence storage format.
    pub html: &'a str,
}

/// The anchor macro that marks an entry, from its key. The same as the Jira
/// app's (`coderhelm-jira`, before it was Jira-only), so entries it published
/// are found and replaced, not repeated.
pub fn anchor_for(key: &str) -> String {
    let mut slug = String::from("coderhelm-");
    let mut run = false;
    for c in key.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
            slug.push(c);
            run = false;
        } else if !run {
            slug.push('-');
            run = true;
        }
    }
    format!(
        "<ac:structured-macro ac:name=\"anchor\"><ac:parameter ac:name=\"\">{slug}</ac:parameter></ac:structured-macro>"
    )
}

/// The year page with the entry for `key` put in: in place of its earlier
/// copy, else right after the page's heading, else at the top.
pub fn upsert_entry(page: &str, key: &str, html: &str) -> String {
    let anchor = anchor_for(key);
    let block = format!("{anchor}{html}");
    if let Some(at) = page.find(&anchor) {
        let after = at + anchor.len();
        let own_heading = page[after..].find("<h3").map(|i| after + i);
        let next_anchor = page[after..]
            .find("<ac:structured-macro ac:name=\"anchor\">")
            .map(|i| after + i);
        let next_heading = own_heading.and_then(|h| page[h + 3..].find("<h3").map(|i| h + 3 + i));
        let end = [next_anchor, next_heading]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(page.len());
        return format!("{}{block}{}", &page[..at], &page[end..]);
    }
    if let Some(h1) = page.find("</h1>") {
        let cut = h1 + "</h1>".len();
        return format!("{}{block}{}", &page[..cut], &page[cut..]);
    }
    format!("{block}{page}")
}

/// A Confluence site reached with an account's email and API token.
pub struct Site<'a> {
    pub site: &'a str,
    pub email: &'a str,
    pub token: &'a str,
    pub http: &'a dyn Transport,
}

impl Site<'_> {
    async fn call(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value, String> {
        let root = check_site(self.site).map_err(|e| e.to_string())?;
        let auth = format!(
            "Basic {}",
            STANDARD.encode(format!("{}:{}", self.email, self.token))
        );
        let reply = self
            .http
            .send(Request {
                method,
                url: format!("{root}{path}"),
                headers: vec![
                    ("Authorization".into(), auth),
                    ("Accept".into(), "application/json".into()),
                ],
                body,
            })
            .await
            .map_err(|e| e.to_string())?;
        match reply.status {
            200..=299 => Ok(reply.json().unwrap_or(Value::Null)),
            401 => Err("Confluence didn't accept the email and API token saved for the Atlassian connection.".into()),
            403 => Err("The Atlassian account whose token is saved can't edit pages under that parent page.".into()),
            404 => Err("Confluence has no page with that parent id (or the account can't see it).".into()),
            409 => Err("409".into()),
            s => Err(format!("Confluence answered {s}.")),
        }
    }

    async fn child_by_title(&self, parent: &str, title: &str) -> Result<Option<String>, String> {
        let mut cursor = String::new();
        for _ in 0..20 {
            let path = if cursor.is_empty() {
                format!("/wiki/api/v2/pages/{parent}/children?limit=250")
            } else {
                format!("/wiki/api/v2/pages/{parent}/children?limit=250&cursor={cursor}")
            };
            let page = self.call(Method::Get, &path, None).await?;
            if let Some(hit) = page["results"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|p| p["title"].as_str() == Some(title))
            {
                return Ok(hit["id"].as_str().map(str::to_string));
            }
            let next = page["_links"]["next"].as_str().unwrap_or("");
            match next.split("cursor=").nth(1) {
                Some(c) => {
                    cursor = c.split('&').next().unwrap_or("").to_string();
                    if cursor.is_empty() {
                        return Ok(None);
                    }
                }
                None => return Ok(None),
            }
        }
        Ok(None)
    }

    async fn child(
        &self,
        space: &str,
        parent: &str,
        title: &str,
        seed: &str,
    ) -> Result<String, String> {
        if let Some(id) = self.child_by_title(parent, title).await? {
            return Ok(id);
        }
        let made = self
            .call(
                Method::Post,
                "/wiki/api/v2/pages",
                Some(json!({
                    "spaceId": space,
                    "status": "current",
                    "title": title,
                    "parentId": parent,
                    "body": { "representation": "storage", "value": seed },
                })),
            )
            .await?;
        made["id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| "Confluence made the page but sent no id.".to_string())
    }

    /// Publishes the entry; returns the year page's address.
    pub async fn publish(&self, e: &Entry<'_>) -> Result<String, String> {
        let parent = check_page_id(e.parent_id).map_err(|e| e.to_string())?;
        if e.year.is_empty() || e.key.is_empty() || e.html.is_empty() {
            return Err("A release entry needs its year, key and text.".into());
        }
        let p = self
            .call(Method::Get, &format!("/wiki/api/v2/pages/{parent}"), None)
            .await?;
        let space = p["spaceId"].as_str().unwrap_or("").to_string();
        let container = if e.container.trim().is_empty() {
            "Changelog"
        } else {
            e.container.trim()
        };
        let container_id = self
            .child(
                &space,
                parent,
                container,
                &format!("<h1>{container}</h1><p>Release notes published by CoderHelm, one page per year.</p>"),
            )
            .await?;
        let year_id = self
            .child(
                &space,
                &container_id,
                e.year,
                &format!("<h1>{}</h1>", e.year),
            )
            .await?;
        for attempt in 0..2 {
            let page = self
                .call(
                    Method::Get,
                    &format!("/wiki/api/v2/pages/{year_id}?body-format=storage"),
                    None,
                )
                .await?;
            let current = page["body"]["storage"]["value"].as_str().unwrap_or("");
            let next = upsert_entry(current, e.key, e.html);
            let version = page["version"]["number"].as_i64().unwrap_or(0);
            match self
                .call(
                    Method::Put,
                    &format!("/wiki/api/v2/pages/{year_id}"),
                    Some(json!({
                        "id": year_id,
                        "status": "current",
                        "title": page["title"],
                        "body": { "representation": "storage", "value": next },
                        "version": { "number": version + 1, "message": format!("Release notes: {}", e.key) },
                    })),
                )
                .await
            {
                Ok(_) => {
                    let base = page["_links"]["base"].as_str().unwrap_or("");
                    let webui = page["_links"]["webui"].as_str().unwrap_or("");
                    return Ok(if base.is_empty() || webui.is_empty() {
                        "published".into()
                    } else {
                        format!("{base}{webui}")
                    });
                }
                Err(m) if m == "409" && attempt == 0 => continue,
                Err(m) if m == "409" => {
                    return Err("Someone edited the year page at the same moment twice. Publish again.".into())
                }
                Err(m) => return Err(m),
            }
        }
        Err("The year page couldn't be updated.".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::http::fake::Fake;

    #[test]
    fn an_entry_goes_under_the_heading_and_replaces_itself() {
        let page = "<h1>2026</h1><p>old</p>";
        let once = upsert_entry(page, "acme/web@v1", "<h3>v1</h3><p>a</p>");
        assert!(once.starts_with("<h1>2026</h1><ac:structured-macro"));
        assert!(once.ends_with("<h3>v1</h3><p>a</p><p>old</p>"));
        let again = upsert_entry(&once, "acme/web@v1", "<h3>v1</h3><p>b</p>");
        assert_eq!(again.matches("coderhelm-acme-web-v1").count(), 1);
        // The Jira app's own anchor for the same key.
        assert!(anchor_for("acme/web@v1 x").contains(">coderhelm-acme-web-v1-x<"));
        assert!(again.contains("<p>b</p>") && !again.contains("<p>a</p>"));
        let two = upsert_entry(&again, "acme/web@v2", "<h3>v2</h3>");
        assert!(
            two.find("v2").unwrap() < two.find("<h3>v1").unwrap(),
            "newest first"
        );
        assert_eq!(
            upsert_entry("", "k", "<p>x</p>"),
            format!("{}<p>x</p>", anchor_for("k"))
        );
    }

    #[tokio::test]
    async fn publishing_makes_the_container_and_year_and_writes_the_entry() {
        let root = "https://example.atlassian.net";
        let year = json!({"id": "300", "title": "2026", "version": {"number": 1}, "body": {"storage": {"value": "<h1>2026</h1>"}}, "_links": {"base": format!("{root}/wiki"), "webui": "/spaces/X/pages/300"}}).to_string();
        let f = Fake::answering(vec![
            (200, r#"{"id": "100", "spaceId": "7"}"#),
            (200, r#"{"results": [{"id": "200", "title": "Changelog"}]}"#),
            (200, r#"{"results": []}"#),
            (200, r#"{"id": "300"}"#),
            (200, &year),
            (200, "{}"),
        ]);
        let s = Site {
            site: root,
            email: "bot@example.com",
            token: "t",
            http: &f,
        };
        let url = s
            .publish(&Entry {
                parent_id: "100",
                container: "",
                year: "2026",
                key: "acme/web@v1",
                html: "<h3>v1</h3>",
            })
            .await
            .unwrap();
        assert_eq!(url, format!("{root}/wiki/spaces/X/pages/300"));
        let sent = f.sent();
        let made = sent.iter().find(|r| r.method == Method::Post).unwrap();
        assert_eq!(made.body.as_ref().unwrap()["parentId"], "200");
        assert_eq!(made.body.as_ref().unwrap()["title"], "2026");
        let put = sent.iter().find(|r| r.method == Method::Put).unwrap();
        assert_eq!(put.body.as_ref().unwrap()["version"]["number"], 2);
        assert!(put.body.as_ref().unwrap()["body"]["value"]
            .as_str()
            .unwrap()
            .contains("<h3>v1</h3>"));
        assert!(sent.iter().all(|r| r
            .headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v.starts_with("Basic "))));
    }
}
