//! Where [`crate::conversation`] reads and writes: the shared Jira channel (the
//! team's Jira app, with a ledger of what CoderHelm said, since a Jira
//! comment can't carry a hidden marker) and GitHub's conversation as read
//! from its comments (a hidden marker on each of CoderHelm's).
//!
//! These, and the worker's channel over its GitHub client, are the only
//! places that call the trackers' comment endpoints (a source scan checks
//! it: `worker/src/passes/conversation.rs`).

use crate::conversation::{self, Channel, Heard, How, Meta, Place, Said, View};
use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::{json, Value};

fn ms_of(rfc3339: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .map(|t| t.timestamp_millis())
        .unwrap_or(0)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// The thread as GitHub lists its comments: CoderHelm's own (a bot's,
/// carrying the marker) and people's (not bots).
pub fn view_from_github(comments: &[Value]) -> View {
    let mut view = View::default();
    for c in comments {
        let body = c["body"].as_str().unwrap_or("");
        let id = match &c["id"] {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            _ => continue,
        };
        let bot = c["user"]["type"].as_str() == Some("Bot")
            || c["user"]["login"]
                .as_str()
                .is_some_and(|l| l.ends_with("[bot]"));
        if bot {
            if let Some(meta) = conversation::read_marker(body) {
                let at = c["updated_at"]
                    .as_str()
                    .or(c["created_at"].as_str())
                    .map(ms_of)
                    .unwrap_or(0);
                view.said.push(Said {
                    id,
                    at_ms: at,
                    meta,
                    body: conversation::strip_marker(body),
                });
            }
            continue;
        }
        view.heard.push(Heard {
            id,
            author: c["user"]["login"].as_str().unwrap_or("").to_string(),
            text: body.to_string(),
            at_ms: c["created_at"].as_str().map(ms_of).unwrap_or(0),
            url: c["html_url"].as_str().unwrap_or("").to_string(),
            // A review comment (a line of the diff) carries its path.
            line: c.get("path").is_some_and(|p| p.is_string()),
            ..Default::default()
        });
    }
    view
}

// ── Jira ──────────────────────────────────────────────────────────────

/// The ledger's sort key for a thread.
pub fn ledger_sk(place: &Place) -> String {
    format!("{}{}", conversation::SK_PREFIX, place.key())
}

/// A Jira ticket, written through the team's Jira app (`add_comment_url`).
/// What CoderHelm said is kept in the settings table; what people said
/// comes from the ticket's kept comments (`ticket_comments`).
pub struct JiraChannel<'a> {
    pub http: &'a reqwest::Client,
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub settings_table: &'a str,
    pub team_id: &'a str,
    pub url: String,
    pub ticket: String,
    /// The Jira app's panel (`error`, `success`, `info`, ...) and its
    /// title.
    pub comment_type: String,
    pub title: String,
}

impl JiraChannel<'_> {
    async fn said(&self) -> Result<Vec<Said>, String> {
        let item = self
            .dynamo
            .get_item()
            .table_name(self.settings_table)
            .key("pk", AttributeValue::S(self.team_id.to_string()))
            .key("sk", AttributeValue::S(ledger_sk(&self.place())))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| format!("{e:?}").chars().take(300).collect::<String>())?
            .item;
        Ok(item
            .as_ref()
            .and_then(|i| i.get("doc"))
            .and_then(|v| v.as_s().ok())
            .and_then(|d| serde_json::from_str(d).ok())
            .unwrap_or_default())
    }

    async fn keep(&self, id: &str, replaced: Option<&str>, meta: &Meta, body: &str) {
        let mut said = self.said().await.unwrap_or_default();
        said.retain(|s| s.id != id && Some(s.id.as_str()) != replaced);
        said.push(Said {
            id: id.to_string(),
            at_ms: now_ms(),
            meta: meta.clone(),
            // Kept short: enough to rewrite a question as answered.
            body: crate::truncate_str(body, 4_000).to_string(),
        });
        let drop = said.len().saturating_sub(conversation::MAX_SAID);
        said.drain(..drop);
        let ttl = chrono::Utc::now().timestamp() + 90 * 86_400;
        if let Err(e) = self
            .dynamo
            .put_item()
            .table_name(self.settings_table)
            .item("pk", AttributeValue::S(self.team_id.to_string()))
            .item("sk", AttributeValue::S(ledger_sk(&self.place())))
            .item(
                "doc",
                AttributeValue::S(serde_json::to_string(&said).unwrap_or_default()),
            )
            .item("ttl", AttributeValue::N(ttl.to_string()))
            .send()
            .await
        {
            tracing::warn!(ticket = %self.ticket, error = ?e, "Could not keep what CoderHelm said on the ticket");
        }
    }

    async fn send(
        &self,
        body: &str,
        edit: Option<&str>,
        reply_to: Option<&Heard>,
    ) -> Result<(String, bool), String> {
        let mut payload = json!({
            "issueKey": self.ticket,
            "comment": body,
            "commentType": conversation::jira_panel(&self.comment_type),
            "title": if conversation::jira_panel(&self.comment_type).is_empty() {
                ""
            } else {
                self.title.as_str()
            },
        });
        if let Some(id) = edit {
            payload["commentId"] = json!(id);
        }
        // The Jira app turns the person's name that opens the reply into a
        // mention, and quotes their comment (an older app shows the name).
        if let Some(h) = reply_to {
            payload["replyTo"] = json!({
                "commentId": h.id,
                "accountId": h.author_id,
                "author": h.author,
            });
        }
        let resp = self
            .http
            .post(&self.url)
            .header("Content-Type", "application/json")
            .body(payload.to_string())
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!(
                "Jira comment failed ({status}): {}",
                crate::truncate_str(&text, 300)
            ));
        }
        let v: Value = resp.json().await.unwrap_or(Value::Null);
        let id = match &v["id"] {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => String::new(),
        };
        Ok((id, v["edited"].as_bool().unwrap_or(false)))
    }
}

#[async_trait::async_trait]
impl Channel for JiraChannel<'_> {
    fn place(&self) -> Place {
        Place::jira(&self.ticket)
    }

    async fn read(&self) -> Result<View, String> {
        let said = self.said().await?;
        let store = crate::ticket_comments::Dynamo {
            client: self.dynamo,
            table: self.settings_table,
        };
        let heard = crate::ticket_comments::Store::of_ticket(&store, self.team_id, &self.ticket)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|c| Heard {
                id: c.comment_id,
                author: c.author,
                author_id: c.author_id,
                text: c.text,
                at_ms: ms_of(&c.at),
                ..Default::default()
            })
            .collect();
        Ok(View { said, heard })
    }

    async fn create(&self, body: &str, meta: &Meta) -> Result<String, String> {
        let (id, _) = self.send(body, None, None).await?;
        if !id.is_empty() {
            self.keep(&id, None, meta, body).await;
        }
        Ok(id)
    }

    async fn edit(&self, id: &str, body: &str, meta: &Meta) -> Result<String, String> {
        let (written, edited) = self.send(body, Some(id), None).await?;
        // A Jira app older than editing posts a new comment instead: that
        // one is the live comment from now on.
        let written = if edited || written.is_empty() {
            id.to_string()
        } else {
            written
        };
        self.keep(&written, Some(id), meta, body).await;
        Ok(written)
    }

    async fn reply(
        &self,
        to: &Heard,
        _how: How,
        body: &str,
        meta: &Meta,
    ) -> Result<String, String> {
        let (id, _) = self.send(body, None, Some(to)).await?;
        if !id.is_empty() {
            self.keep(&id, None, meta, body).await;
        }
        Ok(id)
    }
}

// ── GitHub, with a token (the gateway) ────────────────────────────────

/// An issue or pull request conversation, with an installation token. The
/// worker uses its own client (which counts requests against the
/// installation's budget); the gateway answers people from webhooks with
/// this.
pub struct GithubRest<'a> {
    pub http: &'a reqwest::Client,
    pub token: String,
    pub owner: String,
    pub repo: String,
    pub number: u64,
}

impl GithubRest<'_> {
    fn api(&self) -> String {
        format!("https://api.github.com/repos/{}/{}", self.owner, self.repo)
    }

    async fn call(&self, req: reqwest::RequestBuilder) -> Result<Value, String> {
        let resp = req
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let v: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(format!(
                "GitHub answered {}: {}",
                status.as_u16(),
                v["message"].as_str().unwrap_or("")
            ));
        }
        Ok(v)
    }
}

fn id_of(v: &Value) -> String {
    match &v["id"] {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => String::new(),
    }
}

#[async_trait::async_trait]
impl Channel for GithubRest<'_> {
    fn place(&self) -> Place {
        Place::github(&self.owner, &self.repo, self.number)
    }

    async fn read(&self) -> Result<View, String> {
        let mut all = Vec::new();
        for page in 1..=3u32 {
            let url = format!(
                "{}/issues/{}/comments?per_page=100&page={page}",
                self.api(),
                self.number
            );
            let v = self.call(self.http.get(url)).await?;
            let batch = v.as_array().cloned().unwrap_or_default();
            let done = batch.len() < 100;
            all.extend(batch);
            if done {
                break;
            }
        }
        Ok(view_from_github(&all))
    }

    async fn create(&self, body: &str, meta: &Meta) -> Result<String, String> {
        let url = format!("{}/issues/{}/comments", self.api(), self.number);
        let v = self
            .call(
                self.http
                    .post(url)
                    .json(&json!({ "body": conversation::with_marker(body, meta) })),
            )
            .await?;
        Ok(id_of(&v))
    }

    async fn react(&self, to: &Heard, content: &str) -> Result<(), String> {
        let kind = if to.line { "pulls" } else { "issues" };
        let url = format!("{}/{kind}/comments/{}/reactions", self.api(), to.id);
        self.call(self.http.post(url).json(&json!({ "content": content })))
            .await
            .map(|_| ())
    }

    async fn reply(&self, to: &Heard, how: How, body: &str, meta: &Meta) -> Result<String, String> {
        if how == How::Thread {
            let url = format!(
                "{}/pulls/{}/comments/{}/replies",
                self.api(),
                self.number,
                to.id
            );
            let v = self
                .call(self.http.post(url).json(&json!({ "body": body })))
                .await?;
            return Ok(id_of(&v));
        }
        self.create(body, meta).await
    }

    async fn edit(&self, id: &str, body: &str, meta: &Meta) -> Result<String, String> {
        let url = format!("{}/issues/comments/{id}", self.api());
        self.call(
            self.http
                .patch(url)
                .json(&json!({ "body": conversation::with_marker(body, meta) })),
        )
        .await?;
        Ok(id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn github_comments_split_into_coderhelms_and_peoples() {
        let meta = Meta {
            slot: "run".into(),
            fp: "f1".into(),
            ..Default::default()
        };
        let comments = vec![
            json!({"id": 1, "user": {"login": "coderhelm[bot]", "type": "Bot"}, "body": conversation::with_marker("Opened #5.", &meta), "created_at": "2026-11-01T10:00:00Z", "updated_at": "2026-11-01T10:05:00Z"}),
            json!({"id": 2, "user": {"login": "sam", "type": "User"}, "body": "Thanks, but use the other endpoint.", "created_at": "2026-11-01T10:10:00Z"}),
            json!({"id": 3, "user": {"login": "ci-bot[bot]", "type": "Bot"}, "body": "Coverage 80%", "created_at": "2026-11-01T10:11:00Z"}),
        ];
        let v = view_from_github(&comments);
        assert_eq!(v.said.len(), 1);
        assert_eq!(v.said[0].meta, meta);
        assert_eq!(v.said[0].at_ms, ms_of("2026-11-01T10:05:00Z"));
        assert_eq!(v.heard.len(), 1);
        assert_eq!(v.heard[0].author, "sam");
        assert_eq!(v.unheard().len(), 1);
    }
}
